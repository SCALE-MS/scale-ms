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

The following diagram uses the :py:mod:`scalems.radical` execution module
to illustrate the workflow execution.

.. uml::

    title scalems on radical.pilot (client side)

    participant "workflow script" as script
    box "SCALE-MS framework" #honeydew
    participant "SCALE-MS API" as scalems.Runtime
    participant WorkflowManager as client_workflowmanager
    participant Queuer
    end box
    box "SCALE-MS RP adapter" #linen
    participant scalems.radical
    participant Executor as client_executor
    end box

    autoactivate on

    -> scalems.radical: python -m scalems.radical ...

    scalems.radical -> scalems.Runtime: scalems.invocation.run(workflow_manager)
    scalems.Runtime -> scalems.radical: configuration()
    return
    scalems.Runtime -> scalems.radical: workflow_manager()
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

    ...Dispatch work. See `manage_execution`...

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

__all__ = ("AbstractWorkflowUpdater", "RuntimeManager", "manage_execution")

import abc
import asyncio
import contextlib
import logging
import typing

import scalems.exceptions
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.messages import QueueItem
from scalems.store import FileStore
from scalems.workflow import Task

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


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


class RuntimeDescriptor:
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

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        if instance is None:
            return self
        else:
            return getattr(instance, self.private_name, None)

    def __set__(self, instance, value):
        if getattr(instance, self.private_name, None) is not None:
            raise APIError("Cannot overwrite an existing runtime state.")
        setattr(instance, self.private_name, value)

    def __delete__(self, instance):
        try:
            delattr(instance, self.private_name)
        except AttributeError:
            pass

    def __init__(self):
        self.private_name = "_runtime_state"


_BackendT = typing.TypeVar("_BackendT")


class RuntimeManager(typing.Generic[_BackendT], abc.ABC):
    """Client side manager for dispatching work loads and managing data flow.

    A RuntimeManager is instantiated within the scope of the
    `scalems.workflow.WorkflowManager.dispatch` context manager using the
    :py:meth:`scalems.workflow.WorkflowManager._executor_factory`.
    """

    get_edit_item: typing.Callable[[], typing.Callable]
    """Get the function that creates a WorkflowItem editing context."""

    datastore: FileStore
    submitted_tasks: typing.MutableSet[asyncio.Task]

    _runtime_configuration: _BackendT

    _command_queue: asyncio.Queue
    _dispatcher_lock: asyncio.Lock
    _queue_runner_task: typing.Union[None, asyncio.Task] = None

    runtime = RuntimeDescriptor()
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
        dispatcher_lock=None,
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

        if not isinstance(dispatcher_lock, asyncio.Lock):
            raise TypeError("An asyncio.Lock is required to control dispatcher state.")
        self._dispatcher_lock = dispatcher_lock

    def configuration(self) -> _BackendT:
        return self._runtime_configuration

    @staticmethod
    async def cpi(command: str, runtime):
        """Dispatcher for CPI messages.

        TODO: Return value? We probably want to be able to capture something we can
            query for the result of the CPI message.
        """
        logger.debug(f"Null CPI handler received command {command}.")

    @staticmethod
    def runtime_shutdown(runtime):
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

    def queue(self):
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
        # TODO: Convert from an abstract method to a registration pattern.
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
                assert not self._dispatcher_lock.locked()
                async with self._dispatcher_lock:
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

                # Note: it would probably be most useful to return something with a
                # WorkflowManager interface...
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
        # The dispatching protocol is immature.
        # Initially, we don't expect contention for the lock, and if there is
        # contention, it probably represents an unintended race condition
        # or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        assert not self._dispatcher_lock.locked()
        async with self._dispatcher_lock:
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
    queue = executor.queue()
    updater = executor.updater()
    # Get a reference to the runtime, since it is removed from the executor before
    # RuntimeManager.__aexit__() shuts down this task.
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
                    await executor.cpi(command["control"], runtime)
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


# TODO: Resolve circular reference between `execution` and `workflow` modules.
# class ExecutorFactory(typing.Protocol[_BackendT]):
#     def __call__(self,
#                  manager: WorkflowManager,
#                  params: typing.Optional[_BackendT] = None) -> RuntimeManager[_BackendT]:
#         ...
