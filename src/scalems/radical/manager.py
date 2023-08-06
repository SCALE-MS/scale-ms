"""RADICAL Pilot runtime management for the scalems client."""

from __future__ import annotations

__all__ = ("launch", "RuntimeManager")

import asyncio
import atexit
import concurrent.futures
import contextlib
import contextvars
import dataclasses
import functools
import logging
import os
import queue as _queue
import threading
import typing
import weakref
from time import monotonic as monotonic

from radical import pilot as rp

import scalems.exceptions
import scalems.execution
import scalems.messages
import scalems.radical
import scalems.workflow
import scalems.radical.raptor
from scalems.exceptions import DispatchError
from scalems.exceptions import MissingImplementationError
from scalems.identifiers import EphemeralIdentifier
from scalems.messages import Control
from scalems.radical.exceptions import RPConfigurationError
from scalems.radical.runtime_configuration import get_pre_exec
from scalems.radical.runtime_configuration import RuntimeConfiguration
from scalems.radical.session import runtime_session
from scalems.radical.session import RuntimeSession
from scalems.store import FileStore

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


# The following infrastructure logic is borrowed from concurrent.futures.thread
# What we really want, though, is to make sure that these hooks are handled before
#  the event loop is closed.
# TODO: Make sure that event loop is still active, and perform our shutdown hooks
#  before the event loop shuts down.
_runtime_queues = weakref.WeakKeyDictionary()
"""Keep track of the active CPI queues in the current runtime.

Analogous to the way concurrent.futures.thread tracks the ThreadPoolExecutor
threads and work queues.
"""

_shutdown = False
# Lock that ensures that new executors are not created while the interpreter is
# shutting down. Must be held while mutating _runtime_queues and _shutdown.
# Warning: don't copy this while locked, such as by forking.
_global_shutdown_lock = threading.Lock()


def _python_exit():
    global _shutdown
    with _global_shutdown_lock:
        _shutdown = True
    items = list(_runtime_queues.items())
    # TODO: Update to something that will allow completion of anything that was
    #  trying to get the _global_shutdown_lock or that is affected by _shutdown=True
    for t, q in items:
        q.put(None)
    for t, q in items:
        t.join()


atexit.register(_python_exit)


@contextlib.asynccontextmanager
async def async_lock_proxy(
    lock: threading.Lock,
    *,
    event_loop: asyncio.AbstractEventLoop,
    executor: typing.Optional[concurrent.futures.ThreadPoolExecutor] = None,
):
    """Acquire a `threading.Lock` from a coroutine without blocking the event loop thread.

    Uses the event loop's default ThreadPoolExecutor to offload the thread-safe lock
    acquisition to a non-main thread. Note that there are a finite number of threads
    in the thread pool, so it is possible that a coroutine waiting to enter this
    context manager may not actually be a candidate to acquire the lock until other
    tasks submitted to the thread pool have run.
    """
    acquired = False
    try:
        acquired = await event_loop.run_in_executor(executor=executor, func=lock.acquire)
        yield acquired
    finally:
        if acquired:
            lock.release()


class CommandItem:
    """CPI Command queue item.

    Describes the command to be processed by the CPI servicer. Provides state
    for dispatching through RP Raptor and receives results.

    Used internally to support :py:func:`RuntimeManager.cpi()`
    """

    message: scalems.messages.Command
    future: concurrent.futures.Future


@dataclasses.dataclass(frozen=True)
class ResourceToken:
    """Represent allocated resources.

    By using an identifiable object instead of just a native value type, the
    issuer can identify distinct but equivalent allocations.
    """

    cpu_cores: int


class RuntimeManager:
    """Runtime resources for SCALE-MS workflow execution, backed by RADICAL Pilot.

    Initialize with a Session and pre-configured Pilot job.

    Executors are provisioned in the allocated resources and user configuration
    of a RuntimeManager.

    The RuntimeManager mediates interaction between Executors and WorkflowManager
    to fulfil Futures and update workflow state.

    We expect to have Executor state somewhat disconnected from the calling program
    scope, so the RuntimeManager context is an opportunity to collect Tasks bound
    to different executors, to cleanly shut down executors and release resources.

    The RuntimeManager implements the Computing Provider Interface (CPI) for SCALE-MS
    calls to the runtime provider.`

    The context provided by the RuntimeManager to the ScalemsExecutor is intended
    to be analogous to the multiprocessing context used by
    `concurrent.futures.ProcessPoolExecutor`. The RuntimeManager itself
    acts like a sort of ResourcePoolExecutor in which the ScalemsExecutor is nested,
    but RuntimeManager is actually modeled more closely on :py:class:`asyncio.Server`.

    Instead of instantiating RuntimeManager directly, use :py:func:`launch()`.
    """

    runtime_configuration: RuntimeConfiguration
    runtime_session: RuntimeSession = scalems.execution.RuntimeDescriptor()

    def __init__(
        self,
        *,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration: RuntimeConfiguration,
        runtime: scalems.radical.session.RuntimeSession,
    ):
        if not isinstance(configuration.target_venv, str) or len(configuration.target_venv) == 0:
            raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

        self._exception = None
        # Note that the event loop in the root thread has a default ThreadPoolExecutor,
        # but we may want a separate small (single-thread?) ThreadPoolExecutor to
        # compartmentalize offloading of sequenced (queued) RP commands. We might even
        # prefer a ProcessPoolExecutor so that we can definitively terminate a hung
        # RP UI (e.g. SIGTERM).
        self._loop: asyncio.AbstractEventLoop = loop
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

        # TODO: Connect with WorkflowManager to query workflow state and to provide Task updates.
        # if editor_factory is None or not callable(editor_factory):
        #     raise TypeError("Provide a callable that produces an edit_item interface.")
        # self.get_edit_item = editor_factory

        if datastore is None:
            raise TypeError("Provide a datastore.")
        self.datastore = datastore

        self.runtime_configuration = configuration

        self.runtime_session = runtime

        resource_pool = {"pilot_cores": 0, "max_cores": 0}
        resource_pool_lock = threading.Lock()

        def apply_runtime_resources(f: asyncio.Future):
            if f.cancelled() or f.exception():
                return
            rm_info: scalems.radical.session.RmInfo = f.result()
            with resource_pool_lock:
                resource_pool["pilot_cores"] += rm_info["requested_cores"]
                resource_pool["max_cores"] = rm_info["requested_cores"]

        runtime.resources.add_done_callback(apply_runtime_resources)

        self._resource_pool = resource_pool
        self._resource_pool_lock = resource_pool_lock
        self._resource_pool_update_condition = threading.Condition(resource_pool_lock)
        self._tokens = weakref.WeakKeyDictionary()
        self._cpi_runners = dict[str, threading.Thread]()

    def acquire(
        self,
        *,
        cores: int,
        timeout: typing.Optional[float] = None,
        # subscriber: SomeInterface
    ):
        """Lock resources from the RuntimeManager, such as for a RPExecutor.

        Warnings:
            Blocks blocks the current thread until the resource becomes available.
            (Consider dispatching through a `concurrent.futures.ThreadPoolExecutor`,
            such as with `asyncio.to_thread`.)

        Returns:
            ResourceToken that MUST be provided to a `release()` method call to return resource to the pool.
        """
        # Optional limit on remaining time to wait
        wait_time = timeout
        if wait_time is not None:
            end_time = monotonic() + wait_time
        else:
            end_time = None
        with self.shutdown_lock():
            if self._shutdown:
                raise scalems.exceptions.ScopeError("Cannot allocate resources while shutting down.")
            if not self.runtime_session.resources.done():
                if threading.current_thread() == threading.main_thread():
                    raise scalems.exceptions.APIError(
                        "RuntimeManager.acquire() may only be called in the main (event loop) thread "
                        "after RuntimeSession.resources.done() is True. (Use a ThreadPoolExecutor instead.)"
                    )
                wait_for_resources = asyncio.run_coroutine_threadsafe(
                    asyncio.wait_for(self.runtime_session.resources, timeout), self._loop
                )
                # Warning: the following could still TimeoutError if the event loop is very congested.
                wait_for_resources.result(timeout=timeout + 1 if timeout is not None else None)
            if cores > self._resource_pool["max_cores"]:
                raise RPConfigurationError("Requested resources cannot be acquired.")
            cv = self._resource_pool_update_condition
            with cv:
                # Interval for checking exit conditions and providing logger output.
                interval = 30
                while not cv.wait_for(lambda: self._resource_pool["pilot_cores"] >= cores, timeout=interval):
                    if wait_time is not None:
                        wait_time = end_time - monotonic()
                        if wait_time <= 0:
                            raise scalems.exceptions.DispatchError(
                                f"Requested resources cannot be acquired in {timeout} seconds."
                            )
                        if wait_time < interval:
                            interval = wait_time + 1
                    if self._shutdown:
                        logger.info("RuntimeManager {self} began shutting down before resources could be acquired.")
                        return
                    logger.debug(f"Waiting to acquire {cores} cores from {self}.")
                self._resource_pool["pilot_cores"] -= cores
                token = ResourceToken(cpu_cores=cores)
                _finalizer = weakref.finalize(
                    token,
                    logger.critical,
                    f"{token} abandoned without calling RuntimeManager.release()! Leaking {cores} cores.",
                )
                self._tokens[token] = _finalizer
                return token

    def release(self, token: typing.Optional[ResourceToken]):
        """Return resources to the pool.

        MUST be called after an `acquire()`.
        """
        if token is None:
            # During shutdown, a pending `acquire()` may produce a null token.
            return
        with self._resource_pool_update_condition:
            if token not in self._tokens:
                logger.error(f"{token} is not valid with this RuntimeManager.")
                return
            self._tokens[token].detach()
            del self._tokens[token]
            self._resource_pool["pilot_cores"] += token.cpu_cores
            self._resource_pool_update_condition.notify_all()

    async def get_cpi_session(self):
        """Get CPI facilities for a new resource sub-allocation.

        Starts a task in a separate thread to manage the life cycle of a Raptor task
        and the associated CPI command queue.

        Returns the CPI servicer for use with `cpi()`.

        By the time this call returns, the queue for CPI commands is created and
        maintained. A new threaded task has been launched, but may not yet be active.
        Moreover, the RP Raptor, Pilot, or Session could end before queued commands
        are processed. It is essential that the caller regularly check for error states.
        """
        datastore = self.datastore
        raptor_input_task = asyncio.create_task(
            scalems.radical.raptor.raptor_input(filestore=datastore), name="get-raptor-input"
        )
        resource_token = await asyncio.to_thread(self.acquire, cores=1)
        if resource_token is None:
            raptor_input_task.cancel()
            return

        try:
            async with self.async_shutdown_lock():
                if self.closing():
                    raise scalems.exceptions.APIError("RuntimeManager closed before CPI session could be launched.")
                runtime_session = self.runtime_session
                pilot: rp.Pilot = runtime_session.pilot()
                logger.debug(f"Using RP Session {repr(runtime_session)}")
                pre_exec = get_pre_exec(self.runtime_configuration)
                raptor_pre_exec = list(pre_exec)

                # TODO: Wait for usable Pilot+TaskManager?
                assert isinstance(pilot, rp.Pilot)

                raptor: rp.raptor_tasks.Raptor = await scalems.radical.raptor.coro_get_scheduler(
                    pre_exec=raptor_pre_exec,
                    pilot=pilot,
                    filestore=datastore,
                    config_future=raptor_input_task,
                )  # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
                # logger.debug(f"Got Raptor {raptor} in pilot {pilot}.")

            # TODO(#383): Keep checking Session, Pilot, Raptor, and RuntimeManager in case one expires before we
            #  reach the desired state. (Add some Events to the RuntimeSession facility.)
            # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not
            # check type.
            _task = asyncio.create_task(
                asyncio.to_thread(raptor.wait, state=[rp.states.AGENT_EXECUTING] + rp.FINAL),
                name="check-Master-started",
            )
            await _task
            logger.debug(f"Scheduler {raptor.uid} in state {raptor.state}.")
            # TODO: Generalize the exit status checker for the Master task and perform this
            #  this check at the call site.
            if raptor.state in rp.FINAL:
                if raptor.stdout or raptor.stderr:
                    logger.error(f"raptor.stdout: {raptor.stdout}")
                    logger.error(f"raptor.stderr: {raptor.stderr}")
                logger.debug(str(raptor.as_dict()))
                raise DispatchError(f"Master Task {raptor.uid} unexpectedly reached {raptor.state} during launch.")

            # Items in _work_queue get submitted to RP and are then resolved asynchronously
            # in other threads. In order to know when work is actually completed and resources
            # can be freed, we have to separately account for tasks in flight.
            self._submitted_tasks = set()
            # Consider providing more useful cross-referencing identifier.
            thread_name = f"RuntimeManager-{id(self)}-{manage_raptor.__name__}"

            command_queue = _queue.Queue[typing.Optional[CommandItem]]()

            def _finalizer_callback(_: weakref.ReferenceType):
                command_queue.put(None)

            manager_reference = weakref.ref(self, _finalizer_callback)
            runner_args = (manager_reference,)
            runner_kwargs = {"command_queue": command_queue, "raptor": raptor, "resource_token": resource_token}

            # TODO: Do we need to capture a `contextvars.copy_context()`?
            ctx = contextvars.copy_context()
            func_call = functools.partial(ctx.run, manage_raptor, *runner_args, **runner_kwargs)
            t = threading.Thread(
                name=thread_name,
                target=func_call,
            )
            t.start()
            # The manage_raptor thread has taken ownership of the resource_token now.
            resource_token = None
            self._cpi_runners[raptor.uid] = t
            _runtime_queues[t] = command_queue
            return raptor.uid
        finally:
            if resource_token is not None:
                self.release(resource_token)

    @staticmethod
    async def cpi(command: str, runtime: RuntimeSession, raptor: typing.Optional[rp.Task] = None):
        """Send a control command to the raptor scheduler.

        Implements :py:func:`scalems.execution.RuntimeManager.cpi()`

        TODO: Unify with new raptor rpc features.
        See https://github.com/radical-cybertools/radical.pilot/blob/devel/examples/misc/raptor_simple.py
        """
        timeout = 180

        logger.debug(f'Received command "{command}" for runtime {runtime}.')
        if runtime is None:
            raise scalems.exceptions.ScopeError("Cannot issue control commands without an active RuntimeManager.")
        # raptor: rp.Task = runtime.raptor
        logger.debug(f"Preparing command for {repr(raptor)}.")
        if raptor is None:
            raise scalems.exceptions.ScopeError(
                f"Cannot issue control commands without an active RuntimeManager. {runtime}"
            )
        if raptor.state in rp.FINAL:
            raise scalems.exceptions.ScopeError(f"Raptor scheduler is not available. {repr(raptor)}")
        assert raptor.uid
        message = scalems.messages.Control.create(command)
        td = rp.TaskDescription(
            from_dict={
                "raptor_id": raptor.uid,
                "mode": scalems.radical.raptor.CPI_MESSAGE,
                "metadata": message.encode(),
                "uid": EphemeralIdentifier(),
            }
        )
        logger.debug(f"Submitting {str(td.as_dict())}")
        (task,) = await asyncio.to_thread(runtime.task_manager().submit_tasks, [td])
        logger.debug(f"Submitted {str(task.as_dict())}. Waiting...")
        # Warning: we can't wait on the final state of such an rp.Task unless we
        # _also_ watch the scheduler itself, because there are various circumstances
        # in which the Task may never reach a rp.FINAL state.

        # TODO: Can we do this without so many separate concurrent.futures.Futures from
        #  the event loop's ThreadPoolExecutor?
        command_watcher = asyncio.create_task(
            asyncio.to_thread(task.wait, state=rp.FINAL, timeout=timeout), name="cpi-watcher"
        )

        raptor_watcher = asyncio.create_task(
            asyncio.to_thread(raptor.wait, state=rp.FINAL, timeout=timeout), name="raptor-watcher"
        )
        # If raptor task fails, command-watcher will never complete.
        done, pending = await asyncio.wait(
            (command_watcher, raptor_watcher), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if raptor_watcher in done and command_watcher in pending:
            command_watcher.cancel()
        logger.debug(str(task.as_dict()))
        # WARNING: Dropping the Task reference will cause its deletion.
        return command_watcher

    async def wait_closed(self):
        return await self.runtime_session.wait_closed()

    def closing(self):
        return self._shutdown

    def close(self):
        """Manage tear down of the RADICAL Pilot Session and resources.

        Several aspects of the RP runtime interface use blocking calls.
        This method should be run in a non-root thread (concurrent.futures.Future)
        that the event loop can manage as an asyncio-wrapped task.

        The method should be used along with the wait_closed() method to allow
        the asyncio event loop to clean up.

        Overrides :py:class:`scalems.execution.RuntimeManager`

        Design note:
            Note that shutdown could take a while. To avoid blocking the event loop,
            we need a consistent strategy for dispatching to threads. We need to
            make sure that we can shut down as cleanly as possible even if the
            asyncio default ThreadPoolExecutor cannot provide an available thread.
            will probably dispatch it to a thread.

            For now, we will try to keep a strategy of performing high level lifetime
            management calls (like `close()`) exclusively from the event loop thread,
            and moving blocking calls to a thread pool at the implementation level.
            If this strategy becomes impractical, we will have to take care when
            mixing `concurrent.futures` logic and `asyncio` logic.

        """
        # WARNING: If this call needs to manipulate ContexVars, we need to be
        #   careful about how it is dispatched by the caller.

        with self.shutdown_lock():
            if self._shutdown:
                return
            self._shutdown = True
        runtime: RuntimeSession = self.runtime_session
        logger.debug(f"Shutting down {runtime}")

        # TODO: Collect or cancel outstanding tasks, shut down executors, and
        #  synchronize the workflow state.

        # Resolve any Tasks in this runtime scope and make sure their executor
        # resources are freed.
        ...

        # Note that this coroutine could take a long time and could be
        # cancelled at several points.

        try:
            # TODO: Stop the executors.
            # logger.debug("Stopping workflow execution.")
            ...
        # except asyncio.CancelledError as e:
        #     logger.debug(f"{self.__class__.__qualname__} context manager received cancellation while exiting.")
        #     cancelled_error = e
        # except Exception as e:
        #     logger.exception("Exception while stopping dispatcher.", exc_info=True)
        #     if self._exception:
        #         logger.error("Queuer is already holding an exception.")
        #     else:
        #         self._exception = e
        finally:
            # Catch and report errors from executors, and make any necessary updates to workflow state.
            ...

        session: rp.Session = getattr(runtime, "session", None)
        if session is None:
            raise scalems.exceptions.APIError(f"No Session in {runtime}.")
        if session.closed:
            logger.error("RuntimeSession is already closed?!")
        else:
            runtime.close()

            if session.closed:
                logger.debug(f"Session {session.uid} closed.")
            else:
                logger.error(f"Session {session.uid} not closed!")
        logger.debug("RuntimeSession shut down.")
        # TODO: Disconnect from WorkflowManager.

    @contextlib.contextmanager
    def shutdown_lock(self):
        with _global_shutdown_lock, self._shutdown_lock:
            if _shutdown:
                raise RuntimeError("Shutdown is already in progress.")
            yield self._shutdown_lock

    @contextlib.asynccontextmanager
    async def async_shutdown_lock(self):
        async with (
            async_lock_proxy(lock=_global_shutdown_lock, event_loop=self._loop),
            async_lock_proxy(lock=self._shutdown_lock, event_loop=self._loop),
        ):
            if _shutdown:
                raise RuntimeError("Shutdown is already in progress.")
            yield self._shutdown_lock

    async def __aenter__(self):
        if not _shutdown:
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: C901
        """Clean up at context exit.

        In addition to handling exceptions, clean up any Session resource.

        We also need to make sure that we properly disengage from any queues
        or generators.

        We can also leave flags for ourself to be checked at __await__,
        if there is a Task associated with the Executor.
        """
        # First, do something with exceptions that occurred in the `with` block if we're going to.
        ...
        # Finally, shut down the runtime.
        try:
            self.close()
            await self.wait_closed()
        except Exception as e:
            logger.exception("Exception during RuntimeManager.close().")
            raise e

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False

    def loop(self):
        return self._loop


@contextlib.asynccontextmanager
async def launch(
    *, workflow_manager: scalems.workflow.WorkflowManager, runtime_configuration: RuntimeConfiguration
) -> RuntimeManager:
    """Get a runtime execution context for the managed workflow.

    Configure, launch, and manage the runtime facilities.
    The resulting RuntimeManager context can be used to acquire an
    Executor for handling task submissions.

    Example::

        with scalems.workflow.scope(workflow, close_on_exit=True):
            async with scalems.radical.runtime.launch(workflow, runtime_config) as runtime_context:
                async with scalems.execution.executor(runtime_context, requirements={}) as executor:
                    task: concurrent.futures.Future = executor.submit(fn, *args, **kwargs)
                    task: asyncio.Task = asyncio.create_task(loop.run_in_executor(executor, fn, *args))

    """
    if workflow_manager.closed:
        raise scalems.exceptions.ScopeError("WorkflowManager is closed.")

    # Get Workflow editor factory or connect to WorkflowManager to provide Task updates.
    # TODO: Integrate with WorkflowManager.

    # RuntimeManager.__init__
    if "RADICAL_PILOT_DBURL" not in os.environ:
        raise DispatchError("RADICAL Pilot environment is not available.")

    if not isinstance(runtime_configuration.target_venv, str) or len(runtime_configuration.target_venv) == 0:
        raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

    # self.submitted_tasks = set()
    #
    # # TODO: Only hold a queue in an active context manager.
    # self._command_queue = asyncio.Queue()
    # self._exception = None
    # self._loop: asyncio.AbstractEventLoop = loop
    #
    # if editor_factory is None or not callable(editor_factory):
    #     raise TypeError("Provide a callable that produces an edit_item interface.")
    # self.get_edit_item = editor_factory
    #
    # if datastore is None:
    #     raise TypeError("Provide a datastore.")
    # self.datastore = datastore
    #
    # # TODO: Consider relying on module ContextVars and contextvars.Context scope.
    # self._runtime_configuration = configuration
    #
    # if not isinstance(dispatcher_lock, asyncio.Lock):
    #     raise TypeError("An asyncio.Lock is required to control dispatcher state.")
    # self._dispatcher_lock = dispatcher_lock

    # RuntimeManager.__aenter__
    # Get a lock while the state is changing.
    # Warning: The dispatching protocol is immature.
    # Initially, we don't expect contention for the lock, and if there is
    # contention, it probably represents an unintended race condition
    # or systematic dead-lock.
    # TODO: Clarify dispatcher state machine and remove/replace assertions.
    with _global_shutdown_lock:
        if _shutdown:
            return
        # TODO: Check that we have a FileStore.

        logger.debug("Entering RP dispatching context. Waiting for rp.Session.")

        # Note that any thread-dispatched creation functions or other subtasks
        # in `runtime_session` will see the contextvars.Context state from _before_
        # RuntimeManager takes control of its scope. Be careful to keep in mind
        # appropriate coupling and collective state transitions of RuntimeSession
        # and RuntimeManager.
        _runtime: RuntimeSession = await runtime_session(
            configuration=runtime_configuration, loop=workflow_manager.loop()
        )

        runtime_manager = RuntimeManager(
            datastore=workflow_manager.datastore(),
            loop=workflow_manager.loop(),
            configuration=runtime_configuration,
            runtime=_runtime,
        )
        # TODO: Don't release the _global_shutdown_lock until we have set any
        #  necessary variables for program state, but don't yield
        #  with the lock held.

    # We wrap the RuntimeManager context manager to make it easier to dispatch
    # `close()` to a separate thread and to handle errors, while keeping
    # `RuntimeManager.close()` as simple as possible.
    async with runtime_manager as runtime_context:
        if runtime_context is not None:
            try:
                yield runtime_context

            except Exception as e:
                logger.exception("Unhandled exception while in runtime manager context.")
                raise e

            finally:
                close_task = asyncio.create_task(asyncio.to_thread(runtime_context.close), name="runtime_manager.close")
                # __aexit__
                # runtime_shutdown
                await close_task
                await runtime_context.wait_closed()
                # runtime_exception = runtime_context.exception()
                # if runtime_exception:
                #     if isinstance(runtime_exception, asyncio.CancelledError):
                #         logger.info("Executor cancelled.")
                #     else:
                #         assert not isinstance(runtime_exception, asyncio.CancelledError)
                #         logger.exception("Executor task finished with exception", exc_info=runtime_exception)
                # else:
                #     if not runtime_context.queue().empty():
                #         # TODO: Handle non-empty queue.
                #         # There are various reasons that the queue might not be empty and
                #         # we should clean up properly instead of bailing out or compounding
                #         # exceptions.
                #         # TODO: Check for extraneous extra *stop* commands.
                #         logger.error("Bug: Executor left tasks in the queue without raising an exception.")

                logger.debug(f"Exited {runtime_manager} context.")


def manage_raptor(
    manager_reference: weakref.ReferenceType[RuntimeManager],
    *,
    command_queue: _queue.Queue[CommandItem],
    raptor: rp.raptor_tasks.Raptor,
    resource_token: ResourceToken,
) -> None:
    """Run a task dispatching session for the RP backend.

    Manage a work queue either for RP Raptor tasks or for traditional RP executables.

    Process workflow messages until a stop message is received.

    If *worker_requirements* is not None, the configured RuntimeManager MUST allow
    raptor sessions to be created.

    Warning:
        Run only in a non-root thread!
        This function makes blocking calls that rely on progress in the asyncio
        event loop.

    Returns:
        None because this is run through the `threading.Thread` interface and simply joined.

    Note:
        This is a free function instead of a member function so that it can
        run in a separate thread without coupling tightly to the lifetime of the executor
        or its other members. The function is part of the CPIExecutor implementation,
        and interacts with "private" member variables. Technical concerns might be
        alleviated (while suppressing "private member access" warnings) by making the
        function a @staticmethod, but it seems debatable whether that would help or
        hurt readability.
    """
    _runtime_manager: RuntimeManager = manager_reference()
    if _runtime_manager is None:
        return
    try:
        rp_session = _runtime_manager.runtime_session.session
        pilot = _runtime_manager.runtime_session.pilot()
        del _runtime_manager

        # TODO: Wait for Raptor to start?

        # TODO: Integrate with WorkflowManager

        stop_issued = False
        """Whether or not the queue processed a "stop" command."""
        try:
            # Run queue
            stop_issued = _run_queue(
                work_queue=command_queue,
                manager_reference=manager_reference,
                raptor=raptor,
                submitted_tasks=...,  # Maybe just attach bookkeeping to the Futures in the QueueItems.
                failed=...,  # Some mechanism to propagate errors detected in the runtime state, since we suppress exceptions?
            )
            # We could attach dispatching errors or failure notificatinos to the CPI commands
            # for HELLO or to enter scope and allocate Workers.
        except BaseException:
            logger.critical("Leaving queue runner due to exception.", exc_info=True)

        if (
            raptor is not None
            and raptor.state not in rp.FINAL
            and not rp_session.closed
            and pilot.state not in rp.FINAL
        ):
            # shutdown raptor session:
            _shutdown_raptor(raptor, needs_stop=not stop_issued)
    finally:
        _runtime_manager: RuntimeManager = manager_reference()
        if _runtime_manager is not None and resource_token is not None:
            _runtime_manager.release(resource_token)


def _shutdown_raptor(raptor: rp.raptor_tasks.Raptor, *, needs_stop: bool):
    if needs_stop:
        # TODO: Send a more aggressive control.stop to raptor once we have such a concept
        td = rp.TaskDescription(
            from_dict={
                "raptor_id": raptor.uid,
                "mode": scalems.radical.raptor.CPI_MESSAGE,
                "metadata": Control.create("stop").encode(),
                "uid": EphemeralIdentifier(),
            }
        )
        # TODO: Let the RuntimeManager provide a ThreadPoolExecutor for RP UI calls.
        (task,) = raptor.submit_tasks([td])
    logger.info(f"Waiting for RP Raptor raptor task {raptor.uid} to complete...")
    # Note: 60 seconds may not be long enough to cleanly shut down the raptor session,
    # but we are trying to shut down this thread. To allow more time for clean-up,
    # send an actual control.stop through the work queue (instead of None) and wait
    # for it to complete.
    # TODO(#383): This is pointless unless we know that the Session+Pilot are still active.
    raptor.wait(rp.FINAL, timeout=60)
    if raptor.state not in rp.FINAL:
        # Cancel the raptor.
        logger.warning("Canceling the raptor scheduling task.")
        # Note: the effect of CANCEL is to send SIGTERM to the shell that
        # launched the task process.
        # TODO: Report incomplete tasks and handle the consequences of terminating the
        #  Raptor processes early.
        raptor.cancel()
    # According to the RP docs, task cancellation immediately moves the local
    # Task representation to a state of rp.CANCELED (which is in rp.FINAL), even
    # if asynchronous operations later update the Task to DONE or FAILED.
    # Intermediate updates may even include non-FINAL states, so it is unclear how
    # to know when we the final state of the raptor task will eventually be available.
    # The following code may be misleading.
    final_state = raptor.state
    logger.info(f"Master scheduling task state {final_state}: {repr(raptor)}.")
    if raptor.state not in rp.FINAL:
        logger.critical(f"Raptor task {repr(raptor)} in state {final_state} instead of FINAL.")
    if raptor.stdout:
        # TODO(#229): Fetch actual stdout file.
        logger.debug(raptor.stdout)
    if raptor.stderr:
        # TODO(#229): Fetch actual stderr file.
        # Note that all of the logging output goes to stderr, it seems,
        # so we shouldn't necessarily consider it an error level event.
        logger.debug(raptor.stderr)  # TODO(#108,#229): Receive report of work handled by Master.


def _run_queue(
    *,
    work_queue: _queue.Queue,
    raptor: rp.raptor_tasks.Raptor,
    submitted_tasks: set,
    failed: threading.Event,
    manager_reference: weakref.ReferenceType[RuntimeManager],
):
    """Loop until *work_queue* contains a STOP command or None.

    Arguments:
        work_queue: thread-safe queue of work items. Thread blocks waiting for next item.
        raptor: handle to a Raptor task to which commands should be sent.
        submitted_tasks: in-flight Tasks.
        failed: output Event for failed dispatching
    """
    stop_issued = False
    while True:
        command = work_queue.get(block=True)
        logger.debug(f"Got command queue item {command}.")
        if command is not None:
            logger.debug(f"Processing command {repr(command)}")

            # TODO: Set stop_issued if it is a STOP command.

            if isinstance(command, CommandItem):
                # command.run()
                # Unlike concurrent.futures.ThreadPoolExecutor, the task will
                # not have completed by the time the dispatching function returns.
                # We may have to provide references to the runner or subscribe
                # additional callbacks to the Future so that resources can be
                # freed or errors can be logged, etc.
                submitted_tasks.add(command.future)
                # TODO: Handle queue items.
                raise MissingImplementationError

            if raptor.state in rp.FINAL:
                failed.set()
            else:
                # Don't hang onto the reference while waiting on queue.get()
                del command
                continue

        # If queue item is None, pretend it was a "stop" command, though we
        # don't have a Future to attach to.
        if raptor.state not in rp.FINAL and not stop_issued:
            td = rp.TaskDescription(
                from_dict={
                    "raptor_id": raptor.uid,
                    "mode": scalems.radical.raptor.CPI_MESSAGE,
                    "metadata": Control.create("stop").encode(),
                    "uid": EphemeralIdentifier(),
                }
            )
            # TODO: Let the RuntimeManager provide a ThreadPoolExecutor for RP UI calls.
            (task,) = raptor.submit_tasks([td])
            stop_issued = True
            logger.debug(f"Converted None to RP task with 'stop' command {task}")
            # TODO: Retain and follow up on this Task

        # Allow external tasks to cause this loop to end.
        # Exit if:
        #   - The interpreter is shutting down OR
        #   - The manager that owns the worker has been collected OR
        #   - The manager that owns the worker has been shutdown.
        if (_manager := manager_reference()) is None or _shutdown or _manager._shutdown:
            if _manager is not None:
                _manager._shutdown = True
            # Don't hang onto the reference while waiting on queue.get()
            del _manager
            break
        else:
            # This is the logic in concurrent.futures.ThreadPoolExecutor. I'm not sure
            # why it is useful. If we see this warning in practice, we can try to understand
            # the behavior better and replace this warning with better design notes.
            logger.warning("Executor thread found queued None item, but was not directed to shut down.")
    return stop_issued
