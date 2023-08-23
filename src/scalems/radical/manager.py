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

import scalems.cpi
import scalems.exceptions
import scalems.execution
import scalems.messages
import scalems.radical
import scalems.workflow
import scalems.radical.raptor
from scalems.exceptions import DispatchError
from scalems.identifiers import EphemeralIdentifier
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
_runtime_queues: typing.MutableMapping[threading.Thread, _queue.SimpleQueue] = weakref.WeakKeyDictionary()
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


@dataclasses.dataclass
class CPISession:
    """Collection of details related to the CPI session.

    Design note:
        It may be a mistake to collect all of these facets together. After initial
        testing, let's try to prune this or eliminate it entirely. The one facet
        that does not yet have a more canonical home is the *broken* state.
        However, it is potentially important to associate the *broken* state directly
        with the (subset of) *submitted_tasks*.

        Alternatively, this may represent the beginning of a distinction between the
        RuntimeManager that uses asyncio to coordinate multiple CPI sessions, and a
        more compartmentalized threading oriented CPISession manager.
    """

    command_queue: _queue.SimpleQueue[typing.Optional[CommandItem]]
    raptor: rp.raptor_tasks.Raptor
    resource_token: ResourceToken

    broken: threading.Event = dataclasses.field(default_factory=threading.Event)
    stop: threading.Event = dataclasses.field(default_factory=threading.Event)
    """Whether the CPI Session has been directed to STOP.

    It may be appropriate to expand this attribute into a complete state machine.
    """

    submitted_tasks: set[concurrent.futures.Future] = dataclasses.field(default_factory=set)
    """
    Items in command_queue get submitted to RP and are then resolved asynchronously
    in other threads. In order to know when work is actually completed and resources
    can be freed, we have to separately account for tasks in flight.
    """

    runner: typing.Optional[threading.Thread] = None


@dataclasses.dataclass(frozen=True)
class CPIResult:
    """Value type for the `CommandItem.future`.

    Warning:
        This data structure is an internal implementation detail.
        Container schema subject to change.

    Initial container schema represents the relevant output fields of a
    `rp.Task` submitted to a *raptor_id*.

    TODO: Is ``return_value`` working consistently enough to abandon the other fields?
    """

    exception: str
    exception_detail: str
    exit_code: int
    uid: str
    return_value: dict  # TODO: Normalize schema or encoding
    stdout: str
    stderr: str


class CommandItem:
    """CPI Command queue item.

    Describes the command to be processed by the CPI servicer. Provides state
    for dispatching through RP Raptor and receives results.

    Used internally to support :py:func:`RuntimeManager.cpi()`
    """

    message: scalems.cpi.CpiCall
    future: concurrent.futures.Future[CPIResult]

    def __init__(self, future: concurrent.futures.Future, *, cpi_call: scalems.cpi.CpiCall):
        self.future = future
        self.message = cpi_call

    def run(self, raptor: rp.raptor_tasks.Raptor):
        """Set up the Tasks necessary to fulfill the Future."""
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            if raptor is None or raptor.state in rp.FINAL:
                raise scalems.exceptions.ScopeError("Cannot issue control commands without an active Raptor session.")
            logger.debug(f"Preparing command for {repr(raptor)}.")
            message = scalems.cpi.to_raptor_task_metadata(self.message)
            task_description = rp.TaskDescription(
                from_dict={
                    "raptor_id": raptor.uid,
                    "mode": scalems.radical.raptor.CPI_MESSAGE,
                    "metadata": message,
                    "uid": EphemeralIdentifier(),
                }
            )

            # Warning: RP bookkeeping for callbacks may have unexpected logic.
            # For best results mapping a callable to a Task, use unique object IDs
            # for each callable registered (such as with a local definition or a
            # functools.partial wrapper.
            # TODO: Can we take advantage of RP behavior wrt TaskManager handling of callbacks
            #  to pre-register a single callback for all Tasks before any are submitted?
            _callback = functools.partial(_cpi_task_callback, future=self.future)

            logger.debug(f"Submitting {str(task_description.as_dict())}")
            (task,) = raptor.submit_tasks([task_description])
            # TODO: Either take precautions for Tasks that finish before
            #  we can register callbacks or use new RP signature (TBD) to submit
            #  with the callback already.
            task: rp.Task
            task.register_callback(_callback)
        except BaseException as exc:
            self.future.set_exception(exc)
            # Break a reference cycle with the exception 'exc'
            # noinspection PyMethodFirstArgAssignment
            self = None


class CPIException(scalems.exceptions.ScaleMSError):
    """Something went wrong with the remote evaluation of the CPI command."""


def _cpi_task_callback(task: rp.Task, state, *, future: concurrent.futures.Future):
    """RP Task callback to apply `rp.Task` progress to a Future."""
    # Don't forget to bind kwargs with functools.partial
    if state in rp.FINAL:
        result = CPIResult(
            exit_code=task.exit_code,
            stderr=task.stderr,
            stdout=task.stdout,
            exception=task.exception,
            return_value=task.return_value,
            exception_detail=task.exception_detail,
            uid=task.uid,
        )
        if state == rp.DONE and not result.exception:
            future.set_result(result)
        elif state == rp.CANCELED:
            # TODO: Unregister any callbacks we have registered to rp.Task.cancel()!
            #  Make sure we don't recursively cancel.
            future.cancel()
        elif state == rp.FAILED or result.exception:
            # create an exception and set it.
            # Since we can't really deserialize an exception,
            # and we might have failed without an exception,
            # just throw one here.
            # This stack trace is as good as any.
            try:
                cpi_call = scalems.cpi.from_raptor_task_metadata(task.description["metadata"])
                raise CPIException(f"CPI call {task.uid} failed to process command {cpi_call}")
            except Exception as e:
                future.set_exception(e)
                # There may be other circular references through the exception
                # and the Future that holds it that we may want to break in the future.
                future = None


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

    Notes:
        A `concurrent.futures.Executor` is not expected to be able to cancel a Task
        once it is running. However we have (and may need) such functionality with
        a `radical.pilot` backend. Accordingly, we retain references to the issued
        Futures *even after* they have been removed from the work queue.

    Instead of instantiating RuntimeManager directly, use :py:func:`launch()`.
    """

    _loop: asyncio.AbstractEventLoop

    # TODO: Review loop._start_serving() and loop.add_reader() for inspiration.

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
        self._loop = loop
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
        self._cpi_sessions = dict[str, CPISession]()

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
                # PyCharm 2023.2 seems to have some bugs with optional arguments
                # noinspection PyTypeChecker
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
            logger.debug("RuntimeManager.release() called with null token.")
            return
        if token not in self._tokens:
            logger.error(f"{token} is not valid with this RuntimeManager.")
            return
        logger.debug("Acquiring lock for RuntimeManager._resource_pool_update_condition.")
        with self._resource_pool_update_condition:
            logger.debug(f"Releasing token {token} <object id {id(token)}")
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
            # PyCharm 2023.2 seems to have some bugs with optional arguments
            # noinspection PyTypeChecker
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

            # Consider providing more useful cross-referencing identifier.
            thread_name = f"RuntimeManager-{id(self)}-{manage_raptor.__name__}"

            command_queue = _queue.SimpleQueue[typing.Optional[CommandItem]]()

            # TODO(#383): Robustness to RP and Raptor failures.
            # - [ ]  Failure monitor: If Raptor or parent components finish unexpectedly early, set cpi_session.broken.
            # - [ ] Janitor: Schedule a task such that if cpi_session.broken gets set,
            #   set_exception on submitted and queued tasks.
            # - [ ] If a STOP command completes successfully, cancel the janitor.
            cpi_session = CPISession(command_queue=command_queue, raptor=raptor, resource_token=resource_token)

            def _finalizer_callback(_: weakref.ReferenceType):
                command_queue.put(None)

            manager_reference = weakref.ref(self, _finalizer_callback)
            cpi_session_reference = weakref.ref(cpi_session, _finalizer_callback)

            runner_args = (manager_reference,)
            runner_kwargs = {"cpi_session_reference": cpi_session_reference}

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
            cpi_session.runner = t
            self._cpi_sessions[cpi_session.raptor.uid] = cpi_session
            _runtime_queues[t] = command_queue
            return cpi_session
        finally:
            if resource_token is not None:
                logger.debug("Releasing unused resource token.")
                self.release(resource_token)

    async def cpi(self, session: CPISession, cpi_call: scalems.cpi.CpiCall):
        """Send a control command to the raptor scheduler.

        Replaces :py:func:`scalems.execution.RuntimeManager.cpi()`

        Returns a Future for the resolution of the CPI command.

        TODO: Unify with new raptor rpc features.
        See https://github.com/radical-cybertools/radical.pilot/blob/devel/examples/misc/raptor_simple.py
        """
        # PyCharm 2023.2 seems to have some bugs with optional arguments
        # noinspection PyArgumentList
        async with async_lock_proxy(self._shutdown_lock, event_loop=self._loop):
            if self._shutdown:
                raise scalems.exceptions.APIError("Cannot accept CPI calls after starting shutdown.")
                # Note: for internally managed controls, just put directly to the command queue.
            raptor_id = session.raptor.uid
            # Create a CommandItem. Dispatch the task that will fulfill the Future
            # in the CommandItem. Register any necessary bookkeeping callbacks to the Future.
            # Enqueue the CommandItem and return a reference to the Future.
            # CommandItem can be discarded by the queue processor once its Future has a handler.
            logger.debug(f'Received command "{cpi_call}" for session {raptor_id}.')

            # TODO: Ref Executor.submit() for appropriate locking pattern.
            cpi_runner = self._cpi_sessions[raptor_id].runner
            cpi_queue = _runtime_queues[cpi_runner]

            cpi_future = concurrent.futures.Future()
            # TODO: Add some bookkeeping callbacks.

            queue_item = CommandItem(future=cpi_future, cpi_call=cpi_call)
            # PyCharm 2023.2 seems to have some bugs with optional arguments
            # noinspection PyTypeChecker
            await asyncio.to_thread(cpi_queue.put, queue_item)

            # TODO: Ref Executor.submit() for appropriate health checks.
            return cpi_future

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

        TODO: Decide whether async close is good enough or implement more robustly
            with one or more threads.

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
            for cpi_session in self._cpi_sessions.values():
                cpi_session: CPISession
                while True:
                    # Drain the queue completely. Watch out for real commands that may have
                    # gotten interspersed between `None` items.
                    try:
                        command_item = cpi_session.command_queue.get_nowait()
                        if command_item is not None:
                            command_item.future.cancel()
                    except _queue.Empty:
                        break
                for task in cpi_session.submitted_tasks:
                    task.cancel()
                cpi_session.command_queue.put(None)

        runtime: RuntimeSession = self.runtime_session
        logger.debug(f"Shutting down {runtime}")

        # TODO: Collect outstanding tasks, shut down executors, and
        #  synchronize the workflow state. Resolve any Tasks in this runtime
        #  scope and make sure their executor resources are freed.
        ...

        # Note that this coroutine could take a long time and could be
        # cancelled at several points.

        try:
            logger.debug("Stopping workflow execution.")
            for uid, cpi_session in self._cpi_sessions.items():
                cpi_session.command_queue.put(None)
            # FIXME: Where should the Thread.join() happen and how does that interact with our concurrency models.
            # Note that clean shut down will have access to the event loop and ThreadPoolExecutor, but unclean
            # shut down may have a finalized event loop, a broken or full ThreadPoolExecutor, or a program state
            # from which new threads cannot (or should not) be launched.
            for uid, cpi_session in self._cpi_sessions.items():
                cpi_session.runner.join()
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
        # PyCharm 2023.2 seems to have some bugs with optional arguments
        # noinspection PyArgumentList
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
    cpi_session_reference: weakref.ReferenceType[CPISession],
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
    cpi_session = cpi_session_reference()
    assert isinstance(cpi_session, CPISession)
    raptor = cpi_session.raptor
    resource_token = cpi_session.resource_token
    del cpi_session
    _runtime_manager: RuntimeManager = manager_reference()
    if _runtime_manager is None:
        return
    # Otherwise, be sure to release the ResourceToken.
    try:
        rp_session = _runtime_manager.runtime_session.session
        pilot = _runtime_manager.runtime_session.pilot()
        del _runtime_manager

        # TODO: Wait for Raptor to start?

        # TODO: Integrate with WorkflowManager

        try:
            # Run queue
            _run_queue(
                cpi_session_reference=cpi_session_reference,
                manager_reference=manager_reference,
            )
        except BaseException:
            logger.critical("Leaving queue runner due to exception.", exc_info=True)
        else:
            logger.debug("Queue runner completed.")

        if (
            raptor is not None
            and raptor.state not in rp.FINAL
            and not rp_session.closed
            and pilot.state not in rp.FINAL
        ):
            # shutdown raptor session:
            _shutdown_raptor(cpi_session_reference())
    finally:
        _runtime_manager: RuntimeManager = manager_reference()
        if _runtime_manager is None:
            logger.debug(f"RuntimeManager disappeared before token {resource_token} could be released.")
        else:
            _runtime_manager.release(resource_token)


def _shutdown_raptor(cpi_session: CPISession):
    if cpi_session is None:
        return
    raptor = cpi_session.raptor
    needs_stop = not cpi_session.stop.is_set()
    if needs_stop:
        # TODO: Send a more aggressive control.stop to raptor once we have such a concept
        td = rp.TaskDescription(
            from_dict={
                "raptor_id": raptor.uid,
                "mode": scalems.radical.raptor.CPI_MESSAGE,
                "metadata": scalems.cpi.to_raptor_task_metadata(scalems.cpi.stop()),
                "uid": EphemeralIdentifier(),
            }
        )
        cpi_session.stop.set()
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
    cpi_session_reference: weakref.ReferenceType[CPISession],
    manager_reference: weakref.ReferenceType[RuntimeManager],
):
    """Loop until *work_queue* contains a STOP command or None."""
    cpi_session = cpi_session_reference()
    if cpi_session is None:
        return
    command_queue: _queue.SimpleQueue[CommandItem] = cpi_session.command_queue
    raptor: rp.raptor_tasks.Raptor = cpi_session.raptor
    submitted_tasks: set[concurrent.futures.Future] = cpi_session.submitted_tasks
    failed: threading.Event = cpi_session.broken
    stop_issued = cpi_session.stop
    del cpi_session

    while True:
        command = command_queue.get(block=True)
        logger.debug(f"Got command queue item {command}.")
        if command is not None:
            logger.debug(f"Processing command {repr(command)}")

            if isinstance(command, CommandItem):
                # Unlike concurrent.futures.ThreadPoolExecutor, the task will
                # not have completed by the time the dispatching function returns.
                # We may have to provide references to the runner or subscribe
                # additional callbacks to the Future so that resources can be
                # freed or errors can be logged, etc.
                try:
                    command.run(raptor=raptor)
                    # TODO: Need a harder END command.
                    # TODO: Separate responsibilities of (a) managing Raptor and queue from (b) CPI semantics.
                    if command.message == "stop":
                        stop_issued.set()
                except Exception as e:
                    failed.set()
                    command.future.set_exception(e)
                else:
                    # We need to retain references to the tasks in case we need to cancel them.
                    submitted_tasks.add(command.future)
                    command.future.add_done_callback(submitted_tasks.remove)
                # TODO: If we detect a broken program at this point, how should we
                #  bail out? (Ref ThreadPoolExecutor)

            if raptor.state in rp.FINAL and not stop_issued.is_set():
                failed.set()
                logger.debug("Queue runner detected failure.")
            else:
                # Don't hang onto the reference while waiting on queue.get()
                del command
                # TODO: Don't continue processing commands once we believe the CPI
                #  session to be ending.
                continue

        # If queue item is None, pretend it was a "stop" command, though we
        # don't have a Future to attach to.
        if raptor.state not in rp.FINAL and not stop_issued.is_set():
            td = rp.TaskDescription(
                from_dict={
                    "raptor_id": raptor.uid,
                    "mode": scalems.radical.raptor.CPI_MESSAGE,
                    "metadata": scalems.cpi.to_raptor_task_metadata(scalems.cpi.stop()),
                    "uid": EphemeralIdentifier(),
                }
            )
            # TODO: Let the RuntimeManager provide a ThreadPoolExecutor for RP UI calls.
            (task,) = raptor.submit_tasks([td])
            stop_issued.set()
            logger.debug(f"Converted None to RP task with 'stop' command {task}")
            # TODO: Retain and follow up on this Task

        # Allow external tasks to cause this loop to end.
        # Exit if:
        #   - The interpreter is shutting down OR
        #   - The manager that owns the worker has been collected OR
        #   - The manager that owns the worker has been shutdown.
        #   - TODO: The CPI Session is shutting down / Raptor task has ended. Make sure roptor done callback results in a None on the queue and an event we check below.
        if (_manager := manager_reference()) is None or _shutdown or _manager._shutdown:
            if _manager is not None:
                _manager._shutdown = True
            del _manager
            break
        else:
            # This is the logic in concurrent.futures.ThreadPoolExecutor. I'm not sure
            # why it is useful. If we see this warning in practice, we can try to understand
            # the behavior better and replace this warning with better design notes.
            logger.warning("Executor thread found queued None item, but was not directed to shut down.")
