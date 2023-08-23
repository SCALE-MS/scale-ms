"""RP Raptor runtime management for the scalems client.

The following diagram shows the relationships between the WorkflowManager,
the Executor, and the RuntimeManager for a particular execution backend.
For a concrete example of execution on RADICAL Pilot, refer to the
:py:mod:`scalems.radical` execution module documentation.

.. uml::

    title scalems.execution (general case / client side)

    box "SCALE-MS framework" #honeydew
    participant "SCALE-MS API" as scalems.Runtime
    participant WorkflowManager as WorkflowManager
    participant Queuer
    end box
    box "SCALE-MS execution backend" #linen
    participant scalems.radical <<execution module>>
    participant RPDispatchingExecutor as client_executor <<RuntimeManager>>
    end box

    autoactivate on

    scalems.Runtime -> scalems.Runtime: async with dispatch()

    activate WorkflowManager
    note right
        get or initialize WorkflowManager
    end note

    scalems.Runtime -> scalems.radical: executor_factory()
    scalems.radical -> client_executor **: <<create>>
    note left
        move executor factory and dispatch method out of WorkflowManager
    end note

    scalems.radical --> scalems.Runtime: executor
    scalems.Runtime -> Queuer **: <<create>>

    == Launch runtime ==

    scalems.Runtime -> client_executor: async with executor
    activate scalems.Runtime #lightgray
    scalems.Runtime -> Queuer: async with dispatcher
    activate scalems.Runtime #darkgray

    ...Dispatch work. See `manage_execution`...

    scalems.Runtime <-- Queuer
    deactivate scalems.Runtime
    scalems.Runtime <-- client_executor: leave executor context
    deactivate scalems.Runtime
    scalems.Runtime --> scalems.Runtime: end dispatching context

The interface available to ``@scalems.app`` is under development.
See :py:mod:`scalems.workflow`.

The details of work dispatching are not yet strongly specified or fully encapsulated.
`manage_execution` mediates a collaboration between a `RuntimeManager` and a
`WorkflowManager` (via `AbstractWorkflowUpdater`).

.. uml::

    class RuntimeManagerBase

    class RPRuntimeManager

    class RuntimeConfiguration

    class RuntimeSession

    class concurrent.futures.Executor {
    +submit()
    }

    RuntimeManagerBase -up- WorkflowManager

    RPRuntimeManager ..> RuntimeConfiguration

    RPRuntimeManager *- RuntimeSession

    RPRuntimeManager -up-|> RuntimeManagerBase

    ScalemsExecutor .right.|> concurrent.futures.Executor

    RPExecutor -up-|> ScalemsExecutor

    ScalemsExecutor --> RuntimeManagerBase

    RPExecutor -- RPRuntimeManager

    (RPExecutor, RPRuntimeManager) . CPI

"""

from __future__ import annotations

__all__ = ("RPExecutor", "executor", "provision_executor")

import asyncio
import atexit
import concurrent.futures
import contextlib
import contextvars
import functools
import logging
import queue as _queue
import threading
import typing
import warnings
import weakref
from typing import Callable, Sequence, Optional, TypeVar

from typing_extensions import ParamSpec

import radical.pilot as rp

import scalems.cpi
import scalems.execution
from scalems.exceptions import APIError
from scalems.exceptions import MissingImplementationError
from scalems.identifiers import EphemeralIdentifier
from scalems.radical.exceptions import RPConfigurationError
from scalems.radical.manager import RuntimeManager
from scalems.radical.raptor import launch_scheduler
from scalems.radical.raptor import raptor_input
from scalems.radical.runtime_configuration import get_pre_exec

if typing.TYPE_CHECKING:
    # from scalems.radical.session import RuntimeSession
    from weakref import ReferenceType

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


# The following infrastructure logic is borrowed from concurrent.futures.thread
# What we really want, though, is to make sure that these hooks are handled before
#  the event loop is closed.
# TODO: Make sure that event loop is still active, and perform our shutdown hooks
#  before the event loop shuts down.
_rp_work_queues = weakref.WeakKeyDictionary()
"""Keep track of the active executor queues in the current runtime.

Map (weak references to) tasks to the queues in which they are managed.
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
    items = list(_rp_work_queues.items())
    # TODO: Update to something that will allow completion of anything that was
    #  trying to get the _global_shutdown_lock or that is affected by _shutdown=True
    for t, q in items:
        q.put(None)
    for t, q in items:
        t.join()


atexit.register(_python_exit)


class BrokenExecutor(scalems.radical.exceptions.ScaleMSRadicalError):
    """Executor is in an unrecoverable error state and cannot accept new tasks."""


# Note: The scalems implementation will be an adaptation of the scalems.call machinery.
class _RemoteWorkItem:
    """Local representation of work dispatched for remote execution.

    Any exceptions from the remote work will not contain regular stack traces.
    Instead, the message and execption type will be reported, but the exception
    will appear to originate from :py:func:`_RemoteWorkItem.run()`
    """

    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self, *, task_manager: rp.TaskManager, raptor_id: str):
        # This checks and updates the private concurrent.futures.Future._state.
        # We may find that scalems needs more granular state and an additional layer.
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            self.future.set_exception(exc)
            # Break a reference cycle with the exception 'exc'
            # noinspection PyMethodFirstArgAssignment
            self = None
        else:
            self.future.set_result(result)


def _launch_raptor(executor: RPExecutor, worker_requirements):
    if executor is None:
        logger.critical("Executor lost before Raptor session started.")
        return

    runtime_manager: RuntimeManager = executor.runtime_manager()
    # WARNING: runtime_manager could start shutting down at an arbitrary point,
    # and attributes could be set to None.
    with runtime_manager.shutdown_lock():
        if runtime_manager.closing():
            executor._launch_failed()
            return
        event_loop: asyncio.AbstractEventLoop = runtime_manager.loop()
        datastore = runtime_manager.datastore
        rp_session = runtime_manager.runtime_session
        # task_manager = rp_session.task_manager()
        pilot = rp_session.pilot()
        logger.debug(f"Using RP Session {repr(rp_session)}")
        del runtime_manager
    raptor_pre_exec = executor.raptor_pre_exec()

    # Warning: was the datastore lifetime management designed to be thread-safe or do we need a lock?
    f = asyncio.run_coroutine_threadsafe(
        raptor_input(
            filestore=datastore,
        ),
        loop=event_loop,
    )
    # WARNING: May block for a while during start-up.
    logger.debug("Waiting for asyncio coroutine to get raptor_input.")
    config_file = f.result()
    logger.debug(f"Got config file {config_file}")

    # TODO: Wait for usable Pilot+TaskManager?
    assert isinstance(pilot, rp.Pilot)

    # TODO: Wait on a Semaphore or Condition until we know that cores are
    #  or will be available.
    # We should do the RuntimeManager.acquire() before leaving the RPExecutor constructor,
    # or at least before leaving any enclosing context manager, but is RPExecutor.shutdown()
    # the most appropriate place to do the RuntimeManager.release()? Ultimately, we want to
    # do the RuntimeManager.release() as soon as we are sure that all of the tasks that hit
    # this queue will be able to be placed (or are already executing).

    # Get a scheduler task IFF raptor is required.
    runtime_manager: RuntimeManager = executor.runtime_manager()
    if runtime_manager is None:
        logger.error("RuntimeManager lost before Raptor session started.")
        executor._launch_failed()
        return
    logger.debug(f"Launching Raptor in Pilot {pilot}")
    # WARNING: runtime_manager could start shutting down at an arbitrary point,
    # and attributes could be set to None.
    with runtime_manager.shutdown_lock():
        if runtime_manager.closing():
            logger.error("RuntimeManager lost before Raptor session started.")
            executor._launch_failed()
            return
        # WARNING: If we allow the context manager to call RuntimeManager.release() before
        # this point, then we are violating the resource allocation contract.
        # Similarly for any later calls to start Workers.
        # TODO: Additional locking or synchronization and/or
        #  move the resource acquisition/release into this thread.
        raptor: rp.raptor_tasks.Raptor = launch_scheduler(
            pre_exec=raptor_pre_exec,
            pilot=pilot,
            filestore=datastore,
            config_file=config_file,
        )  # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
        logger.debug(f"Got Raptor {raptor} in pilot {pilot}.")
        return raptor


def _run_queue(*, work_queue, executor_reference, raptor: rp.raptor_tasks.Raptor):
    stop_issued = False
    while True:
        # Once we have awaited asyncio.Queue.get(), we _must_ have a corresponding
        # asyncio.Queue.task_done(). For tidiness, we immediately enter a `try` block
        # with a `finally` suite. Don't separate these without considering the Queue
        # protocol.
        # command = await queue.get()
        command = work_queue.get(block=True)
        logger.debug(f"Got command queue item {command}.")
        if command is not None:
            logger.debug(f"Processing command {repr(command)}")

            # TODO: Set stop_issued if it is a STOP command.

            if isinstance(command, _RemoteWorkItem):
                # command.run()
                # Unlike concurrent.futures.ThreadPoolExecutor, the task will
                # not have completed by the time the dispatching function returns.
                # We may have to provide references to the runner or subscribe
                # additional callbacks to the Future so that resources can be
                # freed or errors can be logged, etc.
                _executor = executor_reference()
                if _executor is not None:
                    _executor._submitted_tasks.add(command.future)
                del _executor
                # TODO: Handle queue items.
                raise MissingImplementationError

            if raptor.state in rp.FINAL:
                _executor = executor_reference()
                if _executor is not None:
                    _executor._dispatcher_failed()
                del _executor
            else:
                # Don't hang onto the reference while waiting on queue.get()
                del command
                continue

        # If queue item is None, pretend it was a "stop" command, though we
        # don't have a Future to attach to.
        if raptor.state not in rp.FINAL and not stop_issued:
            stop_issued = True
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
            logger.debug(f"Converted None to RP task with 'stop' command {task}")

        # Allow external tasks to cause this loop to end.
        # Exit if:
        #   - The interpreter is shutting down OR
        #   - The executor that owns the worker has been collected OR
        #   - The executor that owns the worker has been shutdown.
        if (_executor := executor_reference()) is None or _shutdown or _executor._shutdown:
            if _executor is not None:
                _executor._shutdown = True
            # Don't hang onto the reference while waiting on queue.get()
            del _executor
            break  # TODO: As a function, return True if we've sent a STOP command.
        else:
            # This is the logic in concurrent.futures.ThreadPoolExecutor. I'm not sure
            # why it is useful. If we see this warning in practice, we can try to understand
            # the behavior better and replace this warning with better design notes.
            logger.warning("Executor thread found queued None item, but was not directed to shut down.")
    return stop_issued


# Note: This is a free function instead of a member function so that it can
# run in a separate thread without coupling tightly to the lifetime of the executor
# or its other members. The function is part of the RPExecutor implementation,
# and interacts with "private" member variables. Technical concerns might be
# alleviated (while suppressing "private member access" warnings) by making the
# function a @staticmethod, but it seems debatable whether that would help or
# hurt readability.
def manage_raptor(
    executor_reference,
    *,
    work_queue: _queue.Queue,
    worker_requirements: Optional[Sequence[dict]],
    task_requirements: dict,
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
    """
    # raptor: Optional[rp.raptor_tasks.Raptor] = None
    try:
        # Launch raptor session.
        raptor = _launch_raptor(executor_reference(), worker_requirements)
        if raptor is None:
            return
    except BaseException:
        logger.critical("Exception launching raptor session: ", exc_info=True)
        _executor = executor_reference()
        if _executor is not None:
            _executor._launch_failed()
            del _executor
        return

    task_description_template = task_requirements.copy()
    task_description_template.update({"raptor_id": raptor.uid})
    # TODO: Wait for Raptor to start?

    # TODO: Integrate with WorkflowManager

    stop_issued = False
    """Whether or not the queue processed a "stop" command."""
    try:
        # Run queue
        stop_issued = _run_queue(work_queue=work_queue, executor_reference=executor_reference, raptor=raptor)
    except BaseException:
        logger.critical("Leaving queue runner due to exception.", exc_info=True)
    # finally:
    #     # logger.debug('Releasing "{}" from command queue.'.format(str(command)))
    #     # asyncio queues require that we acknowledge that we are done handling the task.
    #     # queue.task_done()
    if raptor is not None and raptor.state not in rp.FINAL:
        raptor: rp.raptor_tasks.Raptor
        # shutdown raptor session:
        if not stop_issued:
            # TODO: Send a more aggressive control.stop to raptor once we have such a concept
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
        logger.info(f"Waiting for RP Raptor raptor task {raptor.uid} to complete...")
        # Note: 60 seconds may not be long enough to cleanly shut down the raptor session,
        # but we are trying to shut down this thread. To allow more time for clean-up,
        # send an actual control.stop through the work queue (instead of None) and wait
        # for it to complete.
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
        _executor = executor_reference()
        if _executor is not None:
            del _executor.raptor
        del _executor


_P = ParamSpec("_P")
_T = TypeVar("_T")


class RPExecutor(scalems.execution.ScalemsExecutor):
    """Provide the `concurrent.futures.Executor` interface for a `radical.pilot` runtime.

    See Also:
        `executor` provides a context manager suited for scalems usage.
    """

    raptor: Optional[rp.raptor_tasks.Raptor] = None

    def __init__(
        self,
        runtime_manager: RuntimeManager,
        *,
        worker_requirements: Optional[Sequence[dict]],
        task_requirements: dict,
    ):
        self.runtime_manager = weakref.ref(runtime_manager)
        self._pre_exec = list(get_pre_exec(runtime_manager.runtime_configuration))

        # self._command_queue = command_queue
        work_queue = _queue.Queue[Optional[_RemoteWorkItem]]()
        self._work_queue = work_queue

        # Note that we will need some sort of semaphore or other mechanism to
        # detect resource contention that should delay the launch of the
        # RP Raptor rp.task. We could acquire a Semaphore for each core we need,
        # but if we can't ensure that no other thread is reserving cores then we
        # could deadlock. A threading.Condition (mediating access to the core accounting)
        # may be more appropriate.
        self._pilot_cores = threading.Semaphore(0)
        # TODO: Subscribe to the RuntimeManager to get this incremented for available
        #  cores and as cores become available.
        # TODO: Release any cores acquired when shutting down!

        self._threads = set()
        self._broken = False
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

        # Items in _work_queue get submitted to RP and are then resolved asynchronously
        # in other threads. In order to know when work is actually completed and resources
        # can be freed, we have to separately account for tasks in flight.
        self._submitted_tasks = set()

        def _finalizer_callback(_: ReferenceType):
            work_queue.put(None)

        runner_args = (weakref.ref(self, _finalizer_callback),)
        # Note: We probably need to make sure that the RPExecutor referent is kept alive
        # (at least if there are any `submit()` calls) until the worker thread doesn't need it
        # (and we're sure no dynamic tasks are adaptively submitting additional tasks to
        # the same executor).
        # TODO: Use contextvars to copy the state of the nested context managers for the launched thread.
        runner_kwargs = {
            "work_queue": work_queue,
            "task_requirements": task_requirements,
            "worker_requirements": worker_requirements,
        }
        # TODO: Do we need to capture a `contextvars.copy_context()`?
        # Consider providing more useful cross-referencing identifier.
        thread_name = f"RPExecutor-{id(self)}-{manage_raptor.__name__}"
        ctx = contextvars.copy_context()
        func_call = functools.partial(ctx.run, manage_raptor, *runner_args, **runner_kwargs)
        t = threading.Thread(
            name=thread_name,
            target=func_call,
        )
        t.start()
        self._queue_runner_task = t
        _rp_work_queues[t] = self._work_queue

    def raptor_pre_exec(self):
        """Shell lines to execute before launching Raptor components.

        Allows inspection of the sequence of shell commands that are inserted
        into RP launch scripts for Raptor Scheduler/Master and Worker Tasks
        managed by this Executor (generated from the RuntimeConfiguration).
        """
        return self._pre_exec.copy()

    def submit(self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> concurrent.futures.Future[_T]:
        """Schedule a function call and get a Future for the result.

        RP Task parameters are determined by the enclosing executor context.
        See `executor()`
        """
        with self._shutdown_lock, self.runtime_manager().shutdown_lock():
            if self._broken:
                raise BrokenExecutor(self._broken)

            if self._shutdown:
                raise RuntimeError("Cannot submit tasks after beginning Executor shutdown")
            if _shutdown:
                raise RuntimeError("Cannot schedule new futures after interpreter shutdown")

            f = concurrent.futures.Future()
            # Note that the submitted function will see the contextvars.Context
            # that was copied when the worker thread was started.
            item = _RemoteWorkItem(f, fn, args, kwargs)

            self._work_queue.put(item)

            # TODO: Somewhere in here, we need to do bookkeeping with respect to
            #  Worker lifetimes and resource requirements.
            self._check_dispatcher_state()
            return f

    def _check_dispatcher_state(self):
        # If we need to lock resources from the RuntimeManager to suppress the
        # launch of subsequent RPExecutor dispatcher tasks, we could do it here.
        # Some things we might check for:
        # * did submit get called after the `executor` context manager ended?
        # * has another RPExecutor come into scope through a new `executor` context manager?
        # * does continuing to submit to this executor cause problems because of the above?
        # This is a place we could deal with the consequences of tasks that generate
        # additional tasks dynamically/adaptively after calling shutdown(wait=False, cancel_futures=False)
        ...

        # If the "worker" has died, react appropriately.
        raptor: rp.raptor_tasks.Raptor = self.raptor
        if raptor.state in rp.FINAL:
            self._dispatcher_failed()

    def _launch_failed(self):
        logger.debug("Executor._launch_failed() triggered.")
        with self._shutdown_lock:
            self._broken = "Raptor session failed to launch. The RPExecutor is not usable."
            # Drain work queue and mark pending futures failed
            while True:
                try:
                    work_item = self._work_queue.get_nowait()
                except _queue.Empty:
                    break
                if work_item is not None:
                    work_item.future.set_exception(BrokenExecutor(self._broken))
                    # TODO: Cancel any RP Tasks that are already submitted.

    def _dispatcher_failed(self):
        logger.debug("Executor._dispatcher_failed() triggered.")
        with self._shutdown_lock:
            self._broken = "Raptor session finished unexpectedly. The RPExecutor is not usable."
            # Drain work queue and mark pending futures failed
            while True:
                try:
                    work_item = self._work_queue.get_nowait()
                except _queue.Empty:
                    break
                if work_item is not None:
                    work_item.future.set_exception(BrokenExecutor(self._broken))
                    # TODO: Cancel any RP Tasks that are already submitted.

    def shutdown(self, wait=True, *, cancel_futures=False):
        """Implement Executor.shutdown()."""
        logger.debug(f"Executor {self} got shutdown(wait={wait}, cancel_futures={cancel_futures})")
        ...
        with self._shutdown_lock:
            # First:
            # Disable task submission and schedule resources to be released once
            # all pending tasks complete.
            if not self._shutdown:
                self._shutdown = True

            # Second:
            if cancel_futures:
                # Drain all work items from the queue, and then cancel their
                # associated futures.
                logger.debug("Canceling Futures.")
                while True:
                    try:
                        work_item = self._work_queue.get_nowait()
                    except _queue.Empty:
                        break
                    if work_item is not None:
                        work_item.future.cancel()

            # Send a wake-up to prevent threads calling
            # _work_queue.get(block=True) from permanently blocking.
            self._work_queue.put(None)
            # TODO: Instead, put an explicit ControlStop message for cleaner behavior.
            # self._work_queue.put(CommandQueueControlItem(command="stop"))
        # Finally:
        if wait:
            # WARNING: We can't block on a concurrent.futures.Future from the event loop thread
            # if it depends on the event loop iterating!
            if threading.current_thread() == threading.main_thread():
                warnings.warn(
                    "Ignoring wait=True in RPExecutor.shutdown() on main thread to avoid event loop deadlock."
                )
            # Wait for dispatcher thread to be done.
            logger.debug(f"Waiting for queue runner thread {self._queue_runner_task} to complete.")
            self._queue_runner_task.join()
            # If the worker thread finishes, it will delete the instance member:
            assert self.raptor is None
            # Wait for tasks to be `done`. RP Tasks complete asynchronously outside of
            # the dispatching function's loop, resolving our Tasks through callbacks.
            # Note that our infrastructure needs to register callbacks that make sure
            # that no Future remains unresolved due to a failure in another task.
            if self._submitted_tasks:
                logger.debug("Resolving remaining Futures.")
                concurrent.futures.wait(
                    self._submitted_tasks, timeout=None, return_when=concurrent.futures.ALL_COMPLETED
                )
        # Shutdown may "complete" while Tasks are still pending. The instance
        # can remain usable as a container for the pending tasks so that the
        # enclosing RuntimeManager scope can finalize the remaining tasks.
        # WARNING: Make sure that the RuntimeManager or a ContextVar keeps the Executor
        # referent alive while the manage_raptor thread or any other collaborators need it,
        # since callers may drop the reference after `shutdown` returns.


async def provision_executor(
    runtime_context: RuntimeManager,
    *,
    worker_requirements: Optional[Sequence[dict]],
    task_requirements: dict,
) -> RPExecutor:
    """Produce an appropriate RPExecutor.

    Inspect *worker_requirements* and *task_requirements* to determine the appropriate
    RP Raptor session to set up.

    Prepare to launch Raptor scheduler and Worker(s), as appropriate.

    Configure the executor so that it is able to appropriately convert scalems task
    submissions to RP Tasks.

    Note that the Raptor components may not be running while the RPExecutor is in
    scope for the caller. Previously configured executors may need to release
    resources before the Raptor components can launch. Since RP does not have a
    native mechanism for sequencing Tasks, scalems must refrain from submitting
    the Raptor session for this executor until scalems can confirm that resources
    will soon be available and that a new Raptor session will not block resources
    needed for other pending Tasks.
    """
    if worker_requirements is not None and not runtime_context.runtime_configuration.enable_raptor:
        raise RPConfigurationError("Raptor is disabled, but worker_requirements were specified.")

    # Inspect task_requirements for compatibility with runtime_context.
    ...

    _executor = RPExecutor(
        runtime_context, worker_requirements=worker_requirements, task_requirements=task_requirements
    )
    logger.debug(f"Provisioned Executor {_executor}")
    return _executor


executor: scalems.execution.ScalemsExecutorFactory


@contextlib.asynccontextmanager
async def executor(
    runtime_context: RuntimeManager,
    *,
    worker_requirements: Optional[Sequence[dict]],
    task_requirements: dict = None,
) -> RPExecutor:
    """Manage an execution configuration scope.

    Provision and launch a ScalemsExecutor for the given resource requirements in
    the provided runtime context.

    Use instead of the regular :py:class:`concurrent.futures.Executor`
    context manager to allow resources to remain bound to incomplete tasks and
    to avoid canceling submitted tasks when leaving the ``with`` block.

    This allows the calling program to submit tasks with different resource requirements
    (in different `executor_scope` contexts) without waiting for previously
    submitted tasks to complete. The executor will register with the RuntimeManager,
    which _will_ ensure that Tasks are done or canceled as it shuts down.

    Args:
        runtime_context (RuntimeManager): The runtime resource context with which to launch the executor.
        task_requirements (dict): pre-configure Task resource requirements with
            `radical.pilot.TaskDescription` fields.
        worker_requirements (optional): provision a Raptor session (where applicable)
            with `radical.pilot.TaskDescription` fields for a set of workers.

    Example::

        with scalems.workflow.scope(workflow, close_on_exit=True):
            with scalems.radical.runtime.launch(workflow, runtime_config) as runtime_context:
                async with scalems.execution.executor(
                        runtime_context,
                        worker_requirements=[{}]*N,
                        task_requirements={}) as executor:
                    # TODO: Establish a uniform interface sooner than later.
                    scalems.submit(fn, *args, **kwargs)
                    # or
                    task: concurrent.futures.Future = executor.submit(fn, *args, **kwargs)
                    # or
                    task: asyncio.Task = asyncio.create_task(loop.run_in_executor(executor, fn, *args))

    """
    # TODO: Generalize and move to scalems.execution
    import scalems.execution

    if (_current_executor := scalems.execution.current_executor.get(None)) is not None:
        raise scalems.exceptions.ProtocolError(f"Executor {_current_executor} is already active.")

    requirements = [task_requirements]
    if worker_requirements is not None:
        requirements.extend(worker_requirements)
    if any(deprecated_key in desc for deprecated_key in ("cpu_processes", "cpu_threads") for desc in requirements):
        message = (
            "Deprecated key used in RP requirements. "
            "Use `ranks` and `cores_per_rank` instead of `cpu_processes` and `cpu_threads`"
        )
        warnings.warn(message, DeprecationWarning)

    # TODO: Move resource acquisition into RPExecutor initializer, probably in the manager_raptor thread itself.
    master_cores = 1
    task_cores = task_requirements.get("ranks", task_requirements.get("cpu_processes", 1)) * task_requirements.get(
        "cores_per_rank", task_requirements.get("cpu_threads", 1)
    )
    if worker_requirements is not None:
        worker_cores = [
            reqs.get("ranks", reqs.get("cpu_processes", 1)) * reqs.get("cores_per_rank", reqs.get("cpu_threads", 1))
            for reqs in worker_requirements
        ]
        cores_required = master_cores + sum(worker_cores)
        if task_cores > max(worker_cores):
            raise RPConfigurationError(f"Cannot run tasks with {task_cores} with worker cores {worker_cores}")
    else:
        cores_required = master_cores + task_cores
    resource_token = await asyncio.to_thread(runtime_context.acquire, cores=cores_required)
    if resource_token is None:
        return
    # end (block to be moved)

    _current_executor: RPExecutor
    _current_executor = await provision_executor(
        runtime_context,
        worker_requirements=worker_requirements,
        task_requirements=task_requirements,
    )

    # FIXME: The Context has already been copied for the manage_raptor thread
    #  in the RPExecutor constructor before we set this ContextVar.
    context_token = scalems.execution.current_executor.set(_current_executor)
    if context_token.old_value is not contextvars.Token.MISSING:
        logger.critical(f"Probable race condition: Executor context already contains {context_token.old_value}")

    logger.debug(f"Entering managed context for Executor {_current_executor}")
    try:
        yield _current_executor
    except Exception as e:
        logger.exception("Unhandled exception while in dispatching context.")
        raise e
    finally:
        logger.debug(f"Finalizing context of Executor {_current_executor}")
        asyncio.create_task(
            asyncio.to_thread(_current_executor.shutdown, wait=False, cancel_futures=False), name="executor_shutdown"
        )
        # Note: copies of the Context will retain references to the executor.
        _context_executor = scalems.execution.current_executor.get(None)
        if _context_executor != _current_executor:
            logger.critical(
                "Race condition in Executor scoping. "
                f"{_context_executor} became active while {_current_executor} was active."
            )
        else:
            scalems.execution.current_executor.reset(context_token)
        # TODO: Make sure the RuntimeManager looks for and handles executor exceptions.
        # executor_exception = _current_executor.exception()
        # if executor_exception:
        #     if isinstance(executor_exception, asyncio.CancelledError):
        #         logger.info("Executor cancelled.")
        #     else:
        #         assert not isinstance(executor_exception, asyncio.CancelledError)
        #         logger.exception("Executor task finished with exception", exc_info=executor_exception)

        # Note: This means that `shutdown` may have already run before Tasks have completed.
        # How do we allow Tasks to submit new subtasks while executing? What does `shutdown`
        # mean if we allow that?
        # We could say that `shutdown` means `submit` may not be called again for _this_
        # executor. We will have a different Executor implementation where tasks are actually
        # executed, and we can document that this `executor_scope` version of the
        # protocol needs to be avoided or used carefully when tasks need to submit tasks.

        if not _current_executor._work_queue.empty():
            # TODO: Handle non-empty queue.
            # There are various reasons that the queue might not be empty and
            # we should clean up properly instead of bailing out or compounding
            # exceptions.
            # TODO: Check for extraneous extra *stop* commands.
            logger.error("Bug: Executor left tasks in the queue without raising an exception.")

        logger.debug(f"Exiting dispatch context for {_current_executor}.")

        # TODO: Release the resources in manage_raptor when we actually know they are freed.
        runtime_context.release(resource_token)
