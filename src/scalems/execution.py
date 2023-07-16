"""Runtime management for the scalems client.

Provide the framework for the SCALE-MS execution middleware layer.

Execution dispatching implementations can be chosen from the command line when
the interpreter is launched with Python's ``-m`` command line option.

Alternatively, the script can use a Python Context Manager (a ``with`` block) to
activate a WorkflowContext with an execution dispatcher implementation.

It is inadvisable to activate an execution dispatching environment with
procedural calls because it is more difficult to make sure that acquired resources
are properly released, in case the interpreter has to terminate prematurely.

Execution dispatching generally uses some sort of concurrency model,
but there is not a consistent concurrency model for all dispatchers.
ScaleMS provides abstractions to insulate scripting from particular implementations
or concurrency primitives (such as the need to call :py:func:`asyncio.run`).

The following diagram shows the relationships between the WorkflowManager,
the Executor, and the RuntimeManager for a particular execution backend.
For a concrete example of execution on RADICAL Pilot, refer to the
:py:mod:`scalems.radical` execution module documentation.

.. uml::

    title scalems on radical.pilot (client side)

    participant "workflow script" as script
    box "SCALE-MS framework" #honeydew
    participant "SCALE-MS API" as scalems.Runtime
    participant WorkflowManager as client_workflowmanager
    participant Queuer
    end box
    box "SCALE-MS execution backend" #linen
    participant scalems.radical <<execution module>>
    participant RPDispatchingExecutor as client_executor <<RuntimeManager>>
    end box

    autoactivate on

    -> scalems.radical: python -m scalems.radical ...

    scalems.radical -> scalems.Runtime: scalems.invocation.run(workflow_manager)
    scalems.Runtime -> scalems.radical: configuration()
    return
    scalems.Runtime -> scalems.radical: workflow_manager(loop)
    note left
        Initialize with event loop
    end note
    scalems.radical -> client_workflowmanager **: <<create>>
    scalems.Runtime <-- scalems.radical:

    scalems.Runtime -> script: <<runpy>>
    return @scalems.app

    scalems.Runtime -> scalems.Runtime: run_dispatch(work, context)
    scalems.Runtime -> client_workflowmanager: async with dispatch()

    client_workflowmanager -> client_workflowmanager: executor_factory()

    client_workflowmanager -> client_executor **: <<create>>
    client_workflowmanager --> client_workflowmanager: executor
    client_workflowmanager -> Queuer **: <<create>>

    == Launch runtime ==

    client_workflowmanager -> client_executor: async with executor
    activate client_workflowmanager #lightgray
    client_workflowmanager -> Queuer: async with dispatcher
    activate client_workflowmanager #darkgray

    ...Dispatch work. See `manage_execution`...

    client_workflowmanager <-- Queuer
    deactivate client_workflowmanager
    client_workflowmanager <-- client_executor: leave executor context
    deactivate client_workflowmanager
    scalems.Runtime <-- client_workflowmanager: end dispatching context
    scalems.Runtime --> scalems.Runtime: loop.run_until_complete()
    deactivate client_workflowmanager
    scalems.Runtime --> scalems.radical: SystemExit.code
    deactivate scalems.Runtime
    <-- scalems.radical: sys.exit

The interface available to ``@scalems.app`` is under development.
See :py:mod:`scalems.workflow`.

The details of work dispatching are not yet strongly specified or fully encapsulated.
`manage_execution` mediates a collaboration between a `RuntimeManager` and a
`WorkflowManager` (via `AbstractWorkflowUpdater`).

"""

from __future__ import annotations

__all__ = ("AbstractWorkflowUpdater", "RuntimeManager", "dispatch", "manage_execution", "Queuer", "ScalemsExecutor")

import abc
import asyncio
import concurrent.futures
import contextlib
import contextvars
import logging
import queue as _queue
import typing_extensions
import typing

import scalems.exceptions
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.messages import CommandQueueAddItem
from scalems.messages import CommandQueueControlItem
from scalems.messages import QueueItem
from scalems.store import FileStore
from scalems.workflow import Task
from scalems.workflow import WorkflowManager

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


_queuer: contextvars.ContextVar = contextvars.ContextVar("_queuer")
"""Identify an asynchronous queued dispatching context.

Non-asyncio-aware functions may need to behave
differently when we know that asynchronous context switching could happen.
We allow multiple dispatchers to be active, but each dispatcher must
1. contextvars.copy_context()
2. set itself as the dispatcher in the new Context.
3. run within the new Context.
4. ensure the Context is destroyed (remove circular references)
"""


_queuer_lock = asyncio.Lock()


current_executor: contextvars.ContextVar[typing.Optional[ScalemsExecutorT]] = contextvars.ContextVar(
    "current_executor", default=None
)
"""Reference the active ScalemsExecutor instance in the current context, if any.

Tasks submitted through an executor will have a copy of the context in which they
were submitted. This module-level state allows utilities like :py:func:`scalems.submit`
and :py:func:`scalems.executable` to operate in terms of the
:py:func:`~scalems.execution.executor()` context manager.

This ContextVar should be manipulated only by the :py:func:`scalems.execution.executor()`
context manager.
"""


class AbstractWorkflowUpdater(abc.ABC):
    # TODO: The `item: Task` argument to `AbstractWorkflowUpdater.submit()` should be
    #    decoupled from the WorkflowManager implementation.
    @abc.abstractmethod
    async def submit(self, *, item: Task) -> asyncio.Task:
        """Submit work to update a workflow item.

        Args:
            item: managed workflow item as produced by
                  WorkflowManager.edit_item()

        Returns:
            watcher task for a coroutine that has already started.

        The returned Task is a watcher task to confirm completion of or allow
        cancellation of the dispatched work. Additional hidden Tasks may be
        responsible for updating the provided item when results are available,
        so the result of this task is not necessarily directly interesting to
        the execution manager.

        The caller check for unexpected cancellation or early failure. If the task has
        not failed or been canceled by the time *submit()* returns, then we can
        conclude that the task has been appropriately bound to the workfow item.
        """
        ...


_RuntimeType = typing.TypeVar("_RuntimeType")


class RuntimeDescriptor(typing.Generic[_RuntimeType]):
    """Data Descriptor class for (backend-specific) runtime state access.

    TODO: Consider making this class generic in terms of the backend/configuration
     type. (Maybe only possible if we keep the subclassing model of RuntimeManagers rather
     than composing type information at instantiation.)
    """

    # Ref: https://docs.python.org/3/reference/datamodel.html#implementing-descriptors
    def __set_name__(self, owner, name):
        # Called by type.__new__ during class creation to allow customization.
        if getattr(owner, self.private_name, None) is not None:
            raise ProtocolError(
                f"Cannot assign {repr(self)} to {owner.__qualname__} "
                f"with an existing non-None {self.private_name} member."
            )

    def __get__(self, instance, owner) -> _RuntimeType | "RuntimeDescriptor":
        # Note that instance==None when called through the *owner* (as a class attribute).
        if instance is None:
            return self
        else:
            return getattr(instance, self.private_name, None)

    def __set__(self, instance, value: _RuntimeType):
        if getattr(instance, self.private_name, None) is not None:
            raise APIError("Cannot overwrite an existing runtime state.")
        setattr(instance, self.private_name, value)

    def __delete__(self, instance):
        try:
            delattr(instance, self.private_name)
        except AttributeError:
            pass

    def __init__(self):
        self.private_name = "_runtime_session"


_BackendT = typing.TypeVar("_BackendT")

_T = typing.TypeVar("_T")
_P = typing_extensions.ParamSpec("_P")


class ScalemsExecutor(concurrent.futures.Executor, abc.ABC):
    """Provide a task launching facility.

    Implements :py:class:`concurrent.futures.Executor` while encapsulating the
    interactions with scalems.runtime.RuntimeManager.
    """

    @abc.abstractmethod
    def submit(
        self, fn: typing.Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> concurrent.futures.Future[_T]:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Implementations of the abstract method should use
        :py:func:`contextvars.copy_context()` to preserve ContextVars from the
        point at which `submit` was called.

        Returns:
            A Future representing the given call.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def shutdown(self, wait=True, *, cancel_futures=False):
        """Shutdown the executor scope.

        Implements `concurrent.futures.Executor.shutdown`.

        After shutdown, the executor instance accepts no new tasks.
        Contrary to the implication for :py:func:`concurrent.futures.Executor.shutdown()`,
        the scalems executor does not explicitly release any resources when shutdown
        is called. Resources are released as the queued work completes.
        """
        # There is no base class implementation to call.
        raise NotImplementedError


ScalemsExecutorT = typing.TypeVar("ScalemsExecutorT", bound=ScalemsExecutor)


class RuntimeManager(typing.Generic[_BackendT, _RuntimeType], abc.ABC):
    """Client side manager for dispatching work loads and managing data flow.

    A RuntimeManager is instantiated for a `scalems.workflow.WorkflowManager`
    using the *executor_factory* provided to `scalems.execution.dispatch`.
    """

    get_edit_item: typing.Callable[[], typing.Callable]
    """Get the function that creates a WorkflowItem editing context."""

    datastore: FileStore
    submitted_tasks: typing.MutableSet[asyncio.Task]

    _runtime_configuration: _BackendT

    _command_queue: asyncio.Queue
    _dispatcher_lock: asyncio.Lock
    _loop: asyncio.AbstractEventLoop
    _queue_runner_task: typing.Union[None, asyncio.Task] = None

    runtime = RuntimeDescriptor[_RuntimeType]()
    """Get/set the current runtime state information.

    Attempting to overwrite an existing *runtime* state raises an APIError.

    De-initialize the stored runtime state by calling ``del`` on the attribute.

    To re-initialize, de-initialize and then re-assign.

    Design Note:
        This pattern allows runtime state objects to be removed from the instance
        data while potentially still in use (such as during clean-up), allowing
        clearer scoping of access to the runtime state object.

        In the future, we may find it more practical to use a module-level "ContextVar"
        and to manage the scope in terms of PEP-567 Contexts. For the initial
        implementation, though, we are using module-level runtime configuration
        information, but storing the state information for initialized runtime facilities
        through this Data Descriptor on the RuntimeManager instance.

    Raises:
        APIError if *state* is not None and a runtime state already exists.

    """

    def __init__(
        self,
        *,
        editor_factory: typing.Callable[[], typing.Callable] = None,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration: _BackendT,
    ):
        self.submitted_tasks = set()

        # TODO: Only hold a queue in an active context manager.
        self._command_queue = asyncio.Queue()
        self._exception = None
        self._loop: asyncio.AbstractEventLoop = loop

        if editor_factory is None or not callable(editor_factory):
            raise TypeError("Provide a callable that produces an edit_item interface.")
        self.get_edit_item = editor_factory

        if datastore is None:
            raise TypeError("Provide a datastore.")
        self.datastore = datastore

        # TODO: Consider relying on module ContextVars and contextvars.Context scope.
        self._runtime_configuration = configuration

    def configuration(self) -> _BackendT:
        return self._runtime_configuration

    @staticmethod
    async def cpi(command: str, runtime):
        """Dispatcher for CPI messages.

        TODO: Return value? We probably want to be able to capture something we can
            query for the result of the CPI message.
        """
        logger.debug(f"Null CPI handler received command {command}.")

    def runtime_shutdown(self, runtime):
        """Shutdown hook for runtime facilities.

        Called while exiting the context manager. Allows specialized handling of
        runtime backends in terms of the RuntimeManager instance and an abstract
        *Runtime* object, presumably acquired while entering the context manager
        through the *runtime_startup* hook.

        The method is static as a reminder that all state should come through
        the *runtime* argument. The hook is used in the context manager
        implemented by the base class, which manages the removal of state
        information regardless of the actions (or errors) of subclasses using
        this hook.
        """
        pass

    def command_queue(self):
        # TODO: Only expose queue while in an active context manager.
        return self._command_queue

    def exception(self) -> typing.Union[None, Exception]:
        return self._exception

    @contextlib.contextmanager
    def runtime_configuration(self):
        """Runtime startup context manager hook.

        This allows arbitrary environment changes during the scope of the __aenter__()
        function, as well as a chance to modify the stored runtime configuration object.

        Warning: this is probably not a permanent part of the interface. We should settle
        on whether to store a configuration/context object or rely on the current
        contextvars.Context.
        """
        ...
        try:
            yield None
        finally:
            ...

    @abc.abstractmethod
    async def runtime_startup(self) -> asyncio.Task:
        """Runtime startup hook.

        If the runtime manager uses a Session, this is the place to acquire it.

        This coroutine itself returns a `asyncio.Task`. This allows the caller to yield until
        until the runtime manager is actually ready.

        Implementations may perform additional checks before returning to ensure that
        the runtime queue processing task has run far enough to be well behaved if it
        later encounters an error or is subsequently canceled. Also, blocking
        interfaces used by the runtime manager could be wrapped in separate thread
        executors so that the asyncio event loop doesn't have to block on this call.
        """
        ...

    @abc.abstractmethod
    def updater(self) -> AbstractWorkflowUpdater:
        """Initialize a WorkflowUpdater for the configured runtime."""
        # TODO(#335): Convert from an abstract method to a registration pattern.
        ...

    async def __aenter__(self):
        with self.runtime_configuration():
            try:
                # Get a lock while the state is changing.
                # Warning: The dispatching protocol is immature.
                # Initially, we don't expect contention for the lock, and if there is
                # contention, it probably represents an unintended race condition
                # or systematic dead-lock.
                # TODO: Clarify dispatcher state machine and remove/replace assertions.
                async with _queuer_lock:
                    runner_task: asyncio.Task = await self.runtime_startup()
                    if runner_task.done():
                        if runner_task.cancelled():
                            raise DispatchError("Runner unexpectedly canceled while starting dispatching.")
                        else:
                            e = runner_task.exception()
                            if e:
                                logger.exception("Runner task failed with an exception.", exc_info=e)
                                raise e
                            else:
                                logger.warning("Runner task stopped unusually early, but did not raise an exception.")
                    self._queue_runner_task = runner_task

                # The returned object needs to provide a Runtime interface for use
                # by the Executor (per scalems.execution.executor()), which is
                # not necessarily `self`.
                return self
            except Exception as e:
                self._exception = e
                raise e

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: C901
        """Clean up at context exit.

        In addition to handling exceptions, clean up any Session resource.

        We also need to make sure that we properly disengage from any queues
        or generators.

        We can also leave flags for ourself to be checked at __await__,
        if there is a Task associated with the Executor.
        """
        # Note that this coroutine could take a long time and could be
        # cancelled at several points.
        cancelled_error = None
        # We don't expect contention for the lock, and if there is
        # contention, it probably represents an unintended race condition
        # or systematic dead-lock.
        async with _queuer_lock:
            runtime = self.runtime
            # This method is not thread safe, but we try to make clear as early as
            # possible that instance.session is no longer publicly available.
            del self.runtime

            try:
                # Stop the executor.
                logger.debug("Stopping workflow execution.")
                # TODO: Make sure that nothing else will be adding items to the
                #  queue from this point.
                # We should establish some assurance that the next line represents
                # the last thing that we will put in the queue.
                self._command_queue.put_nowait({"control": "stop"})
                # TODO: Consider what to do differently when we want to cancel work
                #  rather than just finalize it.

                # Currently, the queue runner does not place subtasks,
                # so there is only one thing to await.
                # TODO: We should probably allow the user to provide some sort of timeout,
                #  or infer one from other time limits.
                try:
                    await self._queue_runner_task
                except asyncio.CancelledError as e:
                    raise e
                except Exception as queue_runner_exception:
                    logger.exception("Unhandled exception when stopping queue handler.", exc_info=True)
                    self._exception = queue_runner_exception
                else:
                    logger.debug("Queue runner task completed.")
                finally:
                    if not self._command_queue.empty():
                        logger.error("Command queue never emptied.")

                    if len(self.submitted_tasks) == 0:
                        logger.debug("No tasks to wait for. Continuing to shut down.")
                    else:
                        # Wait for the tasks.
                        # For scalems.radical, the returned values are the rp.Task
                        # objects in their final states.
                        # QUESTION: How long should we wait before canceling tasks?
                        # TODO: Handle some sort of job maximum wall time parameter.
                        results = await asyncio.gather(*self.submitted_tasks)
                        # TODO: Log something useful about the results.
                        assert len(results) == len(self.submitted_tasks)
            except asyncio.CancelledError as e:
                logger.debug(f"{self.__class__.__qualname__} context manager received cancellation while exiting.")
                cancelled_error = e
            except Exception as e:
                logger.exception("Exception while stopping dispatcher.", exc_info=True)
                if self._exception:
                    logger.error("Queuer is already holding an exception.")
                else:
                    self._exception = e
            finally:
                try:
                    await asyncio.to_thread(self.runtime_shutdown, runtime)
                except asyncio.CancelledError as e:
                    cancelled_error = e
                except Exception as e:
                    logger.exception(f"Exception while shutting down {repr(runtime)}.", exc_info=e)
                else:
                    logger.debug("Runtime resources closed.")
        if cancelled_error:
            raise cancelled_error

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False


@contextlib.asynccontextmanager
async def dispatch(workflow_manager, *, executor_factory, queuer: "Queuer" = None, params=None):
    """Enter the execution dispatching state.

    Attach to a dispatching executor, then provide a scope for concurrent activity.
    This is also the scope during which the RADICAL Pilot Session exists.

    Provide the executor with any currently-managed work in a queue.
    While the context manager is active, new work added to the queue will be picked up
    by the executor. When the context manager is exited, new work will resume
    queuing locally and the remote tasks will be resolved, then the dispatcher
    will be disconnected.

    Currently, we tie the lifetime of the dispatcher to this context manager.
    When leaving the `with` block, we trigger the executor to clean-up and wait for
    its task to complete.
    We may choose some other relationship in the future.

    Args:
        executor_factory: Implementation-specific callable to get a run time work
            manager.
        queuer: A queue processor that will subscribe to the add_item hook to
            feed the executor.
        params: a parameters object relevant to the execution back-end

    .. todo:: Clarify re-entrance policy, thread-safety, etcetera, and enforce.

    """
    if workflow_manager.closed:
        raise scalems.exceptions.ScopeError("WorkflowManager is closed.")

    # 1. Bind a new executor to its queue.
    # 2. Bind a dispatcher to the executor.
    # 3. Enter executor context.
    # 4. Enter dispatcher context.
    #         # 1. (While blocking event loop in UI thread) Install a hook
    #              for the queuer to catch new calls to add_item (the
    #              dispatcher_queue).
    #         # 2. Get snapshot of current workflow state with which to initialize
    #              the executor. (Unblock.)
    #         # 3. Spool workflow snapshot to executor.
    #         # 4. Start dispatcher queue runner.
    #         # 5. Yield.
    # 5. Exit dispatcher context.
    # 6. Exit executor context.
    # TODO: Add lock context for WorkflowManager event hooks
    #  rather than assume the UI and event loop are always in the same thread.

    executor = executor_factory(manager=workflow_manager, params=params)

    # Avoid race conditions while checking for a running dispatcher.
    # TODO: Clarify dispatcher state machine and remove/replace assertions.
    # Warning: The dispatching protocol is immature.
    # Initially, we don't expect contention for the lock,
    # and if there is contention, it probably represents
    # an unintended race condition or systematic dead-lock.
    async with _queuer_lock:
        # Dispatching state may be reentrant, but it does not make sense to
        # re-enter through this call structure.
        if queuer is None:
            queuer = Queuer(source=workflow_manager, command_queue=executor.command_queue())

    try:
        # Manage scope of executor operation with a context manager.
        # RP does not yet use an event loop, but we can use async context manager
        # for future compatibility with asyncio management of network connections,
        # etc.
        #
        # Note: the executor owns a rp.Session during operation.
        async with executor as dispatching_session:
            # Note: *executor* (sms.execution.RuntimeManager) returns itself when
            # "entered", then we yield it below. Now that RuntimeManager is
            # fairly normalized, we could pass the dispatcher to a (new)
            # context manager member function
            # and let the RuntimeManager handle all of this *dispatcher* logic.
            # The WorkflowManager could pass itself as a simpler interface
            # * to the Queuer for the `subscribe` add_item hook and
            # * to the RuntimeManager to provide a WorkflowEditor.edit_item.
            # E.g.
            #     @asynccontextmanager
            #     async def RuntimeManager.manage(
            #       dispatcher: Queuer,
            #       subscriber: WorkflowEditor))
            # Consider also the similarity of RuntimeManager-WorkflowManager-Queuer
            # to a Model-View-Controller.
            async with queuer:
                # We can surrender control here and leave the executor and
                # dispatcher tasks active while evaluating a `with` block suite
                # for the `dispatch` context manager.
                yield dispatching_session
                # When leaving the `with` suite, Queuer.__aexit__ sends a *stop*
                # command to the queue.
            # The *stop* command will be picked up by sms.execution.manage_execution()
            # (as the RuntimeManager's *runner_task*), which will be awaited in
            # RuntimeManager.__exit__().

    except Exception as e:
        logger.exception("Unhandled exception while in dispatching context.")
        raise e

    finally:
        # Warning: The dispatching protocol is immature.
        # Initially, we don't expect contention for the lock,
        # and if there is contention, it probably represents
        # an unintended race condition or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        #       Be on the look-out for nested context managers and usage in
        #       `finally` blocks.

        dispatcher_exception = queuer.exception()
        if dispatcher_exception:
            if isinstance(dispatcher_exception, asyncio.CancelledError):
                logger.info("Dispatching queue processor cancelled.")
            else:
                assert not isinstance(dispatcher_exception, asyncio.CancelledError)
                logger.exception("Queuer encountered exception.", exc_info=dispatcher_exception)
        else:
            if not queuer.queue().empty():
                logger.error(
                    "Queuer finished while items remain in dispatcher queue. "
                    "Approximate size: {}".format(queuer.queue().qsize())
                )

        executor_exception = executor.exception()
        if executor_exception:
            if isinstance(executor_exception, asyncio.CancelledError):
                logger.info("Executor cancelled.")
            else:
                assert not isinstance(executor_exception, asyncio.CancelledError)
                logger.exception("Executor task finished with exception", exc_info=executor_exception)
        else:
            if not executor.command_queue().empty():
                # TODO: Handle non-empty queue.
                # There are various reasons that the queue might not be empty and
                # we should clean up properly instead of bailing out or compounding
                # exceptions.
                # TODO: Check for extraneous extra *stop* commands.
                logger.error("Bug: Executor left tasks in the queue without raising an exception.")

        logger.debug("Exiting {} dispatch context.".format(type(workflow_manager).__name__))


async def manage_execution(executor: RuntimeManager, *, processing_state: asyncio.Event):
    """Process workflow messages until a stop message is received.

    Initial implementation processes commands serially without regard for possible
    concurrency.

    Towards concurrency:
        We can create all tasks without awaiting any of them.

        Some tasks will be awaiting results from other tasks.

        All tasks will be awaiting a `asyncio.Lock` or `asyncio.Condition` for each
        required resource, but must do so indirectly.

        To avoid dead-locks, we can't have a Lock object for each resource unless
        they are managed by an intermediary that can do some serialization of requests.
        In other words, we need a Scheduler that tracks the resource pool,
        packages resource locks only when they can all be acquired without race
        conditions or blocking,
        and which then notifies the Condition for each task that it is allowed to run.

        It should not do so until the dependencies of the task are known to have
        all of the resources they need to complete (running with any dynamic dependencies
        also running) and, preferably, complete.

        Alternatively, the Scheduler can operate in blocks, allocating all resources,
        offering the locks to tasks, waiting for all resources to be released,
        then repeating.
        We can allow some conditions to "wake up" the scheduler to back fill a block
        of resources, but we should be careful with that.

        (We still need to consider dynamic tasks that
        generate other tasks. I think the only way to distinguish tasks which can't be
        dynamic from those which might be would be with the `def` versus `async def` in
        the implementing function declaration. If we abstract `await` with ``scalems.wait``,
        we can throw an exception at execution time after checking a ContextVar.
        It may be better to just let implementers use `await` for dynamically created
        tasks, but we need to make the same check if a function calls ``.result()`` or
        otherwise tries to create a dependency on an item that was not allocated
        resources before the function started executing.
        In a conservative first draft, we can simply throw an exception if
        a non-`async def` function attempts to call a scalems workflow command like
        "add_item" while in an executing context.)

    """
    queue = executor.command_queue()
    updater = executor.updater()
    # Get a reference to the runtime, since it is removed from the executor before
    # RuntimeManager.__aexit__() shuts down this task.
    # Note that the interface for _RuntimeType is not currently well specified.
    runtime = executor.runtime

    # Acknowledge that the coroutine is running and will immediately begin processing
    # queue items.
    processing_state.set()
    # Note that if an exception is raised here, the queue will never be processed.

    # Could also accept a "stop" Event object for the loop conditional,
    # but we would still need a way to yield on an empty queue until either
    # the "stop" event or an item arrives, and then we might have to account
    # for queues that potentially never yield any items, such as by sleeping briefly.
    # We should be able to do
    #     signal_task = asyncio.create_task(stop_event.wait())
    #     queue_getter = asyncio.create_task(command_queue.get())
    #     waiter = asyncio.create_task(asyncio.wait((signal_task, queue_getter),
    #     return_when=FIRST_COMPLETED))
    #     while await waiter:
    #         done, pending = waiter.result()
    #         assert len(done) == 1
    #         if signal_task in done:
    #             break
    #         else:
    #             command: QueueItem = queue_getter.result()
    #         ...
    #         queue_getter = asyncio.create_task(command_queue.get())
    #         waiter = asyncio.create_task(asyncio(wait((signal_task, queue_getter),
    #         return_when=FIRST_COMPLETED))
    #
    command: QueueItem
    while command := await queue.get():
        # Developer note: The preceding line and the following try/finally block are
        # coupled!
        # Once we have awaited asyncio.Queue.get(), we _must_ have a corresponding
        # asyncio.Queue.task_done(). For tidiness, we immediately enter a `try` block
        # with a `finally` suite. Don't separate these without considering the Queue
        # protocol.

        try:
            if not len(command.items()) == 1:
                raise ProtocolError("Expected a single key-value pair.")
            logger.debug(f"Processing command {repr(command)}")

            # TODO(#23): Use formal RPC protocol.
            if "control" in command:
                logger.debug(f"Execution manager received {command['control']} command for {runtime}.")
                try:
                    # TODO(#335): We have to update this model.
                    # await executor.cpi(command["control"], runtime)
                    logger.debug(f"This code path ignores control commands. Dropping {command['control']}.")
                except scalems.exceptions.ScopeError as e:
                    logger.debug(
                        f"Control command \"{command['control']}\" ignored due to inactive RuntimeManager.", exc_info=e
                    )
                    # We should probably break here, too, right?
                    # TODO: Handle command results and take appropriate action on failures.
                if command["control"] == "stop":
                    # End queue processing.
                    break
            if "add_item" not in command:
                # This will end queue processing. Tasks already submitted may still
                # complete. Tasks subsequently added will update the static part of
                # the workflow (WorkflowManager) and be eligible for dispatching in
                # another dispatching session.
                # If we need to begin to address the remaining queued items before
                # this (the queue processor) task is awaited, we could insert a
                # Condition here to alert some sort of previously scheduled special
                # tear-down task.
                raise MissingImplementationError(f"Executor has no implementation for {str(command)}.")

            key = command["add_item"]
            edit_item = executor.get_edit_item()
            with edit_item(key) as item:
                if not isinstance(item, Task):
                    raise InternalError(f"Bug: Expected {edit_item} to return a _context.Task")

                # TODO: Check task dependencies.
                ##
                # Note that we could insert resource management here. We could create
                # tasks until we run out of free resources, then switch modes to awaiting
                # tasks until resources become available, then switch back to placing
                # tasks.

                # Bind the WorkflowManager item to an RP Task.
                task = await updater.submit(item=item)

                if task.done():
                    # Stop processing the queue if task was cancelled or errored.
                    # TODO: Test this logical branch or consider removing it.
                    if task.cancelled():
                        logger.info(f"Stopping queue processing after unexpected cancellation of task {task}")
                        return
                    else:
                        exc = task.exception()
                        if exc:
                            logger.error(f"Task {task} failed much to fast. Stopping execution.")
                            raise exc
                        else:
                            logger.debug(f"Task {task} already done. Continuing.")
                else:
                    executor.submitted_tasks.add(task)

        except Exception as e:
            logger.debug("Leaving queue runner due to exception.")
            raise e
        finally:
            logger.debug('Releasing "{}" from command queue.'.format(str(command)))
            queue.task_done()


class Queuer:
    """Maintain the active dispatching state for a managed workflow.

    The Queuer, WorkflowManager, and Executor lifetimes do not need to be
    coupled, but their periods of activity should be synchronized in certain ways
    (aided using the Python context manager protocol).

    The Queuer must have access to an active Executor while the Queuer
    is active.

    When entering the Queuer context manager, a task is created to transfer items
    from the dispatch queue to the execution queue. The task is allowed to drain the
    queue before the context manager yields to the ``with`` block. The task will
    continue to process the queue asynchronously if new items appear.

    When exiting the Queuer context manager, the items currently in the
    dispatching queue are processed and the the dispatcher task is finalized.

    The dispatcher queue is a queue.SimpleQueue so that WorkflowManager.add_item()
    can easily use a concurrency-safe callback to add items whether or not the
    dispatcher is active.
    """

    command_queue: asyncio.Queue
    """Target queue for dispatched commands."""

    source: WorkflowManager
    """Owner of the workflow items being queued."""

    _dispatcher_queue: _queue.SimpleQueue

    _queue_runner_task: asyncio.Task
    """Queue, owned by this object, of Workflow Items being processed for dispatch."""

    def __init__(self, source: WorkflowManager, command_queue: asyncio.Queue):
        """Create a queue-based workflow dispatcher.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """

        self.source = source
        self._dispatcher_queue = _queue.SimpleQueue()
        self.command_queue = command_queue
        self._exception = None

    def queue(self):
        return self._dispatcher_queue

    def put(self, item: typing.Union[CommandQueueAddItem, CommandQueueControlItem]):
        assert len(item) == 1
        key = list(item.keys())[0]
        if key not in {"command", "add_item"}:
            raise APIError("Unrecognized queue item representation.")
        self._dispatcher_queue.put(item)

    async def __aenter__(self):
        try:
            # Get a lock while the state is changing.
            # Warning: The dispatching protocol is immature.
            # Initially, we don't expect contention for the lock,
            # and if there is contention, it probably represents
            # an unintended race condition or systematic dead-lock.
            # TODO: Clarify dispatcher state machine and remove/replace assertions.
            async with _queuer_lock:
                if _queuer.get(None):
                    raise APIError("There is already an active dispatcher in this Context.")
                _queuer.set(self)
                # Launch queue processor (proxy executor).
                runner_started = asyncio.Event()
                runner_task = asyncio.create_task(self._queue_runner(runner_started))
                await runner_started.wait()
                self._queue_runner_task = runner_task

                # Without yielding,
                # 1. Install a hook for the queuer to catch new calls to add_item.
                # 2. Get snapshot of current workflow state with which to initialize
                #    the executor.
                # Dont' forget to unsubscribe later!
                # self.source_context.subscribe('add_item', self._dispatcher_queue.put)
                self.source.subscribe("add_item", self.put)
                # TODO: Topologically sort DAG!
                initial_task_list = list(self.source.tasks.keys())
                try:
                    for _task_id in initial_task_list:
                        self.command_queue.put_nowait(QueueItem({"add_item": _task_id}))
                except asyncio.QueueFull as e:
                    raise DispatchError("Executor was unable to receive initial commands.") from e
                # It is now safe to yield.

                # TODO: Add lock context for WorkflowManager event hooks
                #  rather than assume the UI and event loop are always in the same thread.

            return self
        except Exception as e:
            self._exception = e
            raise e

    async def _single_iteration_queue(self, source: _queue.SimpleQueue, target: asyncio.Queue):
        """Transfer one queue item.

        If a *stop* command is encountered, self-cancel after transfering command.

        To avoid race conditions while stopping queue processing,
        place a *stop* command in *source* and asyncio.shield() a call
        to this coroutine in a *try: ... except: ...* block.

        Note that the caller will then receive CancelledError after *stop* command has
        been transferred.

        Raises:
            queue.Empty if *source* is empty
            asyncio.CancelledError when cancelled or *stop* is received.

        """
        command: QueueItem = source.get_nowait()
        logger.debug(f"Processing command {repr(command)}")

        await target.put(command)

        # TODO: Use formal RPC protocol.
        if "control" in command:
            # Note that we don't necessarily need to stop managing the dispatcher queue
            # at this point, but the Executor will be directed to shut down,
            # so we must not put anything else onto the command queue until we have a
            # new command queue or a new executor.
            if command["control"] == "stop":
                raise asyncio.CancelledError()
            else:
                raise ProtocolError("Unknown command: {}".format(command["control"]))
        else:
            if "add_item" not in command:
                # TODO: We might want a call-back or Event to force errors before the
                #  queue-runner task is awaited.
                raise MissingImplementationError(f"Executor has no implementation for {str(command)}")
        return command

    async def _queue_runner(self, processing_state: asyncio.Event):
        processing_state.set()
        while True:
            try:
                await asyncio.shield(
                    self._single_iteration_queue(source=self._dispatcher_queue, target=self.command_queue)
                )
            except _queue.Empty:
                # Wait a moment and try again.
                await asyncio.sleep(0.5)

    async def _drain_queue(self):
        """Wait until the dispatcher queue is empty, then return.

        Use in place of join() for event-loop treatment of *queue.SimpleQueue*.
        """
        while not self._dispatcher_queue.empty():
            await asyncio.sleep(0.1)

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: C901
        """Clean up at context exit.

        Drain the dispatching queue and exit.

        Unsubscribes from the WorkflowManager add_item hook, deactivates the
        dispatching context, and exits.
        Does not cancel or send instructions to the Executor managing the command queue.
        """
        # Note that this coroutine could take a long time and could be cancelled at
        # several points.
        cancelled_error = None
        # The dispatching protocol is immature. Initially, we don't expect contention
        # for the lock, and if there is contention, it probably represents an
        # unintended race condition or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        async with _queuer_lock:
            try:
                self.source.unsubscribe("add_item", self.put)
                _queuer.set(None)

                # Stop the dispatcher.
                logger.debug("Stopping the SCALEMS RP dispatching queue runner.")

                # Wait for the queue to drain or the queue runner to exit or fail.
                drain = asyncio.create_task(self._drain_queue())
                done, pending = await asyncio.wait(
                    {drain, self._queue_runner_task}, return_when=asyncio.FIRST_COMPLETED
                )
                assert len(done) > 0
                if self._queue_runner_task not in done:
                    if drain in done:
                        self._queue_runner_task.cancel()
                done, _ = await asyncio.wait({self._queue_runner_task})
                assert self._queue_runner_task in done
                if not self._queue_runner_task.cancelled():
                    exception = self._queue_runner_task.exception()
                else:
                    exception = None
                if exception:
                    logger.exception("Queuer queue processing encountered exception", exc_info=exception)
                    if self._exception:
                        logger.error("Queuer is already holding an exception.")
                    else:
                        self._exception = exception

            except asyncio.CancelledError as e:
                logger.debug("Queuer context manager received cancellation while exiting.")
                cancelled_error = e
            except Exception as e:
                logger.exception("Exception while stopping dispatcher.", exc_info=e)
                if self._exception:
                    logger.error("Queuer is already holding an exception.")
                else:
                    self._exception = e
            finally:
                # Should we do any other clean-up here?
                ...
        if cancelled_error:
            raise cancelled_error

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False

    def exception(self) -> typing.Union[None, Exception]:
        return self._exception


class ScalemsExecutorFactory(typing.Protocol):
    """Get a context manager for scoped execution resources from the runtime manager.

    This scope encompasses the RP Raptor task dispatching, managing the
    life cycle of one Raptor scheduler task and one or more homogeneous Workers
    (with a single resource requirements description).

    The returned executor supports the concurrent.futures.Executor interface,
    and has a unique identifier that maps to the associated Raptor task.

    Example::

        with scalems.workflow.scope(workflow, close_on_exit=True):
            with scalems.radical.runtime.launch(workflow, runtime_config) as runtime_context:
                async with scalems.execution.executor(
                        runtime_context,
                        worker_requirements=[{}]*N,
                        task_requirements={}) as executor:
                    task: concurrent.futures.Future = executor.submit(fn, *args, **kwargs)
                    task: asyncio.Task = asyncio.create_task(loop.run_in_executor(executor, fn, *args))

    """

    def __call__(
        self, runtime_context: RuntimeManager, *args, **kwargs
    ) -> contextlib.AbstractAsyncContextManager[ScalemsExecutor]:
        ...


# TODO: Migrate to new RuntimeManager interface and generalize scalems.radical.runtime.executor.
@contextlib.asynccontextmanager
def executor(
    runtime: RuntimeManager[_BackendT, ScalemsExecutorT],
    *,
    worker_requirements: typing.Sequence[dict],
    task_requirements: dict = None,
) -> ScalemsExecutorT:
    """Get a context manager for scoped execution resources from the runtime manager.

    This scope encompasses the RP Raptor task dispatching, managing the
    life cycle of one Raptor scheduler task and one or more homogeneous Workers
    (with a single resource requirements description).

    The returned executor supports the concurrent.futures.Executor interface,
    and has a unique identifier that maps to the associated Raptor task.

    Example::

        with scalems.workflow.scope(workflow, close_on_exit=True):
            with scalems.radical.runtime.launch(workflow, runtime_config) as runtime_context:
                async with scalems.execution.executor(
                        runtime_context,
                        worker_requirements=[{}]*N,
                        task_requirements={}) as executor:
                    task: concurrent.futures.Future = executor.submit(fn, *args, **kwargs)
                    task: asyncio.Task = asyncio.create_task(loop.run_in_executor(executor, fn, *args))

    Args:
        runtime:
        worker_requirements: A dictionary of worker requirements
            (TaskDescription fields) for each worker to launch
        task_requirements: A dictionary of (homogeneous) task requirements
            (TaskDescription fields) for the tasks to be submitted in the subsequent scope.

    """
