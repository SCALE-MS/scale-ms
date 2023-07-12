"""RP Raptor runtime management for the scalems client."""

from __future__ import annotations

__all__ = ("RPExecutor", "executor", "provision_executor")

import asyncio
import concurrent.futures
import contextlib
import contextvars
import logging
import queue as _queue
import threading
import typing
import warnings
import weakref
from typing import Callable, Sequence, Optional, TypeVar
from typing import Union

from typing_extensions import ParamSpec

import radical.pilot as rp

import scalems.execution
from scalems.exceptions import MissingImplementationError
from scalems.messages import QueueItem
from scalems.radical.exceptions import RPConfigurationError
from scalems.radical.manager import RuntimeManager
from scalems.radical.raptor import get_scheduler
from scalems.radical.runtime_configuration import get_pre_exec

if typing.TYPE_CHECKING:
    from scalems.radical.session import RuntimeSession

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


_queuer: contextvars.ContextVar[Queuer] = contextvars.ContextVar("_queuer")
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


async def manage_execution(
    *,
    processing_state: asyncio.Event,
    command_queue: asyncio.Queue,
    runtime_manager: scalems.radical.manager.RuntimeManager,
    updater: WorkflowUpdaterPlaceholder,
):
    """Process workflow messages until a stop message is received.

    Caller should make sure to handle any remaining queue items if the task throws
    an exception.
    """
    queue = command_queue

    runtime_session: scalems.radical.session.RuntimeSession = runtime_manager.runtime_session

    # TODO: Integrate with WorkflowManager

    # Acknowledge that the coroutine is running and will immediately begin processing
    # queue items.
    processing_state.set()
    # Note that if an exception is raised here, the queue will never be processed.

    command: QueueItem
    while True:
        # Once we have awaited asyncio.Queue.get(), we _must_ have a corresponding
        # asyncio.Queue.task_done(). For tidiness, we immediately enter a `try` block
        # with a `finally` suite. Don't separate these without considering the Queue
        # protocol.
        command = await queue.get()
        try:
            # Allow external tasks to cause this loop to end.
            if command is None:
                return
            if not len(command.items()) == 1:
                raise scalems.exceptions.ProtocolError("Expected a single key-value pair.")
            logger.debug(f"Processing command {repr(command)}")

            # TODO(#23): Use formal RPC protocol.
            if "control" in command:
                logger.debug(f"Execution manager received {command['control']} command for {runtime_session}.")
                try:
                    await runtime_manager.cpi(command["control"], runtime_session)
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

            # TODO: Handle queue items.
            raise MissingImplementationError

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

    # source: WorkflowManager
    # """Owner of the workflow items being queued."""

    _dispatcher_queue: _queue.SimpleQueue

    _queue_runner_task: asyncio.Task
    """Queue, owned by this object, of Workflow Items being processed for dispatch."""

    def __init__(self, source, command_queue: asyncio.Queue):
        """Create a queue-based workflow dispatcher.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """

        # self.source = source
        self._dispatcher_queue = _queue.SimpleQueue()
        self.command_queue = command_queue
        self._exception = None
        self._queuer_context_token: Optional[contextvars.Token] = None

    def queue(self):
        return self._dispatcher_queue

    def put(self, item):
        assert len(item) == 1
        key = list(item.keys())[0]
        if key not in {"command", "add_item"}:
            raise scalems.exceptions.APIError("Unrecognized queue item representation.")
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
                    raise scalems.exceptions.APIError("There is already an active dispatcher in this Context.")
                self._queuer_context_token = _queuer.set(self)
                # Launch queue processor (proxy executor).
                runner_started = asyncio.Event()
                runner_task = asyncio.create_task(self._queue_runner(runner_started))
                await runner_started.wait()
                self._queue_runner_task = runner_task

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
                raise scalems.exceptions.ProtocolError("Unknown command: {}".format(command["control"]))
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
                # self.source.unsubscribe("add_item", self.put)
                _queuer.reset(self._queuer_context_token)

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

    def exception(self) -> Union[None, Exception]:
        return self._exception


_P = ParamSpec("_P")
_T = TypeVar("_T")


class WorkflowUpdaterPlaceholder(scalems.execution.AbstractWorkflowUpdater):
    """Placeholder for the updater required by manage_execution."""

    async def submit(self, *, item: scalems.workflow.Task) -> asyncio.Task:
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

        TODO: This needs to be updated and simplified in the style of
         `scalems.concurrent.Executor.submit()` with appropriate hooks and callbacks.
        """
        raise NotImplementedError


class RPExecutor(scalems.execution.ScalemsExecutor):
    """Provide the `concurrent.futures.Executor` interface for a `radical.pilot` runtime.

    See Also:
        `executor` provides a context manager suited for scalems usage.
    """

    def __init__(
        self, runtime_manager: RuntimeManager, *, command_queue: asyncio.Queue, manage_execution: asyncio.Task
    ):
        self.runtime_manager = weakref.ref(runtime_manager)
        self._pre_exec = list(get_pre_exec(runtime_manager.runtime_configuration))
        self._shutdown_lock = threading.Lock()
        self._shutdown = False

        self._queue_runner_task = manage_execution
        self._command_queue = command_queue
        self._work_queue = _queue.Queue()
        self.submitted_tasks = set()

    def submit(self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> concurrent.futures.Future[_T]:
        """Schedule a function call and get a Future for the result.

        RP Task parameters are determined by the enclosing executor context.
        See `executor()`
        """
        with self._shutdown_lock, self.runtime_manager().shutdown_lock():
            # if self._broken:
            #     raise BrokenThreadPool(self._broken)

            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
            # if _shutdown:
            #     raise RuntimeError("cannot schedule new futures after " "interpreter shutdown")

            # vars_context = contextvars.copy_context()
            f = concurrent.futures.Future()
            # w = _WorkItem(f, fn, args, kwargs)

            # vars_context.run(...)
            # self._work_queue.put(w)
            # self._adjust_thread_count()
            return f

    def shutdown(self, wait=True, *, cancel_futures=False):
        """Implement Executor.shutdown()."""
        ...
        with self._shutdown_lock:
            # First:
            # Disable task submission and schedule resources to be released once
            # all pending tasks complete.
            if not self._shutdown:
                self._shutdown = True
            if not isinstance(self._shutdown, concurrent.futures.Future):
                runtime_manager: RuntimeManager = self.runtime_manager()
                if runtime_manager is None:
                    logger.error("Executor outlived its RuntimeManager.")
                loop: asyncio.AbstractEventLoop = runtime_manager._loop
                self._shutdown = asyncio.run_coroutine_threadsafe(self.finalize(), loop=loop)

            # Second:
            if cancel_futures:
                # Issue a cancel to all incomplete tasks.
                #         # Drain all work items from the queue, and then cancel their
                #         # associated futures.
                #         while True:
                #             try:
                #                 work_item = self._work_queue.get_nowait()
                #             except queue.Empty:
                #                 break
                #             if work_item is not None:
                #                 work_item.future.cancel()
                ...
            # # Send a wake-up to prevent threads calling
            # # _work_queue.get(block=True) from permanently blocking.
            # self._work_queue.put(None)
        # Finally:
        if wait:
            # Wait for tasks to be `done`
            ...
            # WARNING: We can't block on a concurrent.futures.Future from the event loop thread
            # if it depends on the event loop iterating!
            if threading.current_thread() == threading.main_thread():
                warnings.warn("concurrent.futures.Future is probably about to deadlock with the asyncio event loop.")
            self._shutdown.result()
        # Shutdown may "complete" while Tasks are still pending. The instance
        # can remain usable as a container for the pending tasks so that the
        # enclosing RuntimeManager scope can finalize the remaining tasks.

    async def finalize(self):
        """Resolve pending tasks and release resources.

        Wait for Tasks to complete and then shutdown runtime resources.
        """
        # TODO: Allow timed loop for occasional inspection, logging, or forced cancellation.
        if self.submitted_tasks:
            done, pending = await asyncio.wait(self.submitted_tasks, return_when=asyncio.ALL_COMPLETED)
        # TODO: Check final Task status.

        _manager: scalems.radical.manager.RuntimeManager = self.runtime_manager()
        runtime: RuntimeSession = _manager.runtime_session
        session: rp.Session = getattr(runtime, "session", None)
        if session is None:
            raise scalems.exceptions.APIError(f"No Session in {runtime}.")
        if session.closed:
            logger.error("RuntimeSession is already closed?!")

        cancelled_error = None
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
            logger.debug("Queue runner task completed.")
        except asyncio.CancelledError as e:
            logger.debug(f"{self.__class__.__qualname__} context manager received cancellation while exiting.")
            cancelled_error = e
        except Exception:
            logger.exception(f"Exception while stopping {repr(self._queue_runner_task)}.")

        if runtime.raptor is not None:
            # Note: The __aexit__ for the RuntimeManager makes sure that a `stop`
            # is issued after the work queue is drained, if the scheduler task has
            # not already ended. We could check the status of this stop message...
            if runtime.raptor.state not in rp.FINAL:
                logger.info(f"Waiting for RP Raptor raptor task {runtime.raptor.uid} to complete...")
                runtime.raptor.wait(rp.FINAL, timeout=60)
            if runtime.raptor.state not in rp.FINAL:
                # Cancel the raptor.
                logger.warning("Canceling the raptor scheduling task.")
                # Note: the effect of CANCEL is to send SIGTERM to the shell that
                # launched the task process.
                # TODO: Report incomplete tasks and handle the consequences of terminating the
                #  Raptor processes early.
                task_manager = runtime.task_manager()
                task_manager.cancel_tasks(uids=runtime.raptor.uid)
            # As of https://github.com/radical-cybertools/radical.pilot/pull/2702,
            # we do not expect `cancel` to block, so we must wait for the
            # cancellation to succeed. It shouldn't take long, but it is not
            # instantaneous or synchronous. We hope that a minute is enough.
            final_state = runtime.raptor.wait(state=rp.FINAL, timeout=10)
            logger.debug(f"Final state: {final_state}")
            logger.info(f"Master scheduling task state {runtime.raptor.state}: {repr(runtime.raptor)}.")
            if runtime.raptor.stdout:
                # TODO(#229): Fetch actual stdout file.
                logger.debug(runtime.raptor.stdout)
            if runtime.raptor.stderr:
                # TODO(#229): Fetch actual stderr file.
                # Note that all of the logging output goes to stderr, it seems,
                # so we shouldn't necessarily consider it an error level event.
                logger.debug(runtime.raptor.stderr)  # TODO(#108,#229): Receive report of work handled by Master.
        logger.debug("RPExecutor shut down.")

        if cancelled_error:
            raise cancelled_error


async def provision_executor(
    runtime_context: RuntimeManager,
    *,
    worker_requirements: Optional[Sequence[dict]],
    task_requirements: dict,
    command_queue: asyncio.Queue,
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

    _updater = WorkflowUpdaterPlaceholder()

    runner_task: Optional[asyncio.Task] = None
    try:
        runner_started = asyncio.Event()
        runner_task: asyncio.Task = asyncio.create_task(
            manage_execution(
                processing_state=runner_started,
                runtime_manager=runtime_context,
                command_queue=command_queue,
                updater=_updater,
            )
        )
        await runner_started.wait()
        if runner_task.done():
            if runner_task.cancelled():
                raise scalems.exceptions.DispatchError("Runner unexpectedly canceled while starting dispatching.")
            else:
                e = runner_task.exception()
                if e:
                    logger.exception("Runner task failed with an exception.", exc_info=e)
                    raise e
                else:
                    logger.warning("Runner task stopped unusually early, but did not raise an exception.")

        if worker_requirements is None:
            return RPExecutor(runtime_context, command_queue=command_queue, manage_execution=runner_task)

        # else: provision a Raptor master and worker(s)
        _executor = RPExecutor(runtime_context, command_queue=command_queue, manage_execution=runner_task)

        raptor_pre_exec = _executor._pre_exec.copy()

        # Get a scheduler task IFF raptor is required.
        if runtime_context.runtime_configuration.enable_raptor:
            # Note that get_scheduler is a coroutine that, itself, returns a rp.Task.
            # We await the result of get_scheduler, then store the scheduler Task.
            runtime_context.runtime_session.raptor = await asyncio.create_task(
                get_scheduler(
                    pre_exec=raptor_pre_exec,
                    task_manager=runtime_context.runtime_session.task_manager(),
                    filestore=runtime_context.datastore,
                ),
                name="get-scheduler",
            )  # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
        return _executor
    except Exception as e:
        if isinstance(runner_task, asyncio.Task):
            runner_task.cancel()
        if isinstance(runtime_context.runtime_session.raptor, rp.Task):
            runtime_context.runtime_session.raptor.cancel()
        raise e


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

    command_queue = asyncio.Queue()
    # TODO: manage command_queue through which the Executor coordinates with the run time.
    #  It is probably owned by RuntimeManager, which provides the CPI interface.

    requirements = [task_requirements]
    if worker_requirements is not None:
        requirements.extend(worker_requirements)
    if any(deprecated_key in desc for deprecated_key in ("cpu_processes", "cpu_threads") for desc in requirements):
        message = (
            "Deprecated key used in RP requirements. "
            "Use `ranks` and `cores_per_rank` instead of `cpu_processes` and `cpu_threads`"
        )
        warnings.warn(message, DeprecationWarning)

    if worker_requirements is not None:
        worker_cores = sum(
            reqs.get("ranks", reqs.get("cpu_processes", 1)) * reqs.get("cores_per_rank", reqs.get("cpu_threads", 1))
            for reqs in worker_requirements
        )
    else:
        worker_cores = task_requirements.get(
            "ranks", task_requirements.get("cpu_processes", 1)
        ) * task_requirements.get("cores_per_rank", task_requirements.get("cpu_threads", 1))
    master_cores = 1
    cores_required = master_cores + worker_cores
    resource_token = await asyncio.to_thread(runtime_context.acquire, cores=cores_required)
    if resource_token is None:
        return

    _current_executor: RPExecutor
    _current_executor = await provision_executor(
        runtime_context,
        worker_requirements=worker_requirements,
        task_requirements=task_requirements,
        command_queue=command_queue,
    )

    token = scalems.execution.current_executor.set(_current_executor)
    assert token.old_value is contextvars.Token.MISSING

    queuer = Queuer(source=None, command_queue=command_queue)

    try:
        # When leaving the following `with` suite, Queuer.__aexit__ sends a *stop*
        # command to the queue.
        async with queuer:
            # Any exceptions encountered while in the `executor` context manager scope
            # will be injected after the following `yield`. The Queuer has the option
            # to handle and suppress them, but we don't expect it to yet.
            yield _current_executor
        # The *stop* command will be picked up by sms.execution.manage_execution()
        # (as the RuntimeManager's *runner_task*), which will be awaited in
        # RPExecutor.finalize().
    except Exception as e:
        logger.exception("Unhandled exception while in dispatching context.")
        raise e
    finally:
        # Note: copies of the Context will retain references to the executor.
        assert scalems.execution.current_executor.get() == _current_executor
        scalems.execution.current_executor.reset(token)
        asyncio.create_task(
            asyncio.to_thread(_current_executor.shutdown, wait=False, cancel_futures=False), name="executor_shutdown"
        )
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

        if not _current_executor._command_queue.empty() or not _current_executor._work_queue.empty():
            # TODO: Handle non-empty queue.
            # There are various reasons that the queue might not be empty and
            # we should clean up properly instead of bailing out or compounding
            # exceptions.
            # TODO: Check for extraneous extra *stop* commands.
            logger.error("Bug: Executor left tasks in the queue without raising an exception.")

        logger.debug("Exiting dispatch context.")
        runtime_context.release(resource_token)
