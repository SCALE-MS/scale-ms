from __future__ import annotations

__all__ = ("RPExecutor", "executor", "provision_executor")

import concurrent.futures
import contextlib
import contextvars
import logging
from typing import Callable, Sequence, Optional, TypeVar

from typing_extensions import ParamSpec

import scalems.execution
from scalems.exceptions import MissingImplementationError
from scalems.radical.exceptions import RPConfigurationError
from scalems.radical.manager import RuntimeManager
from scalems.radical.session import RuntimeSession

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

_P = ParamSpec("_P")
_T = TypeVar("_T")


class RPExecutor(scalems.execution.ScalemsExecutor):
    def __init__(self, runtime_manager: RuntimeManager):
        # self._shutdown_lock = threading.Lock()
        # self._shutdown = False
        ...

    def submit(self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> concurrent.futures.Future[_T]:
        """Schedule a function call and get a Future for the result.

        RP Task parameters are determined by the enclosing executor context.
        See `executor()`
        """
        future = concurrent.futures.Future()
        return future

    def shutdown(self, wait=True, *, cancel_futures=False):
        ...


async def provision_executor(
    runtime_context: RuntimeManager, *, worker_requirements: Optional[Sequence[dict]], task_requirements: dict
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

    Args:
        *:
    """
    if worker_requirements is not None and not runtime_context.runtime_configuration.enable_raptor:
        raise RPConfigurationError("Raptor is disabled, but worker_requirements were specified.")

    # Inspect task_requirements for compatibility with runtime_context.
    ...

    if worker_requirements is None:
        return RPExecutor(runtime_context)
    else:
        raise MissingImplementationError

    # # Get a scheduler task IFF raptor is required.
    # if runtime_context.runtime_configuration.enable_raptor:
    #     # Note that get_scheduler is a coroutine that, itself, returns a rp.Task.
    #     # We await the result of get_scheduler, then store the scheduler Task.
    #     _runtime.raptor = await asyncio.create_task(
    #         get_scheduler(
    #             #TODO: Executor should have its own pre_exec
    #             pre_exec=list(get_pre_exec(runtime_context.runtime_configuration)),
    #             task_manager=runtime_context.runtime_session.task_manager(),
    #             filestore=runtime_context.datastore,
    #             scalems_env="scalems_venv",
    #             # TODO: normalize ownership of this name.
    #         ),
    #         name="get-scheduler",
    #     )  # Note that we can derive scheduler_name from self.scheduler.uid in later methods.


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
                    scalems.submit(
                    )
                    task: concurrent.futures.Future = executor.submit(fn, *args, **kwargs)
                    task: asyncio.Task = asyncio.create_task(loop.run_in_executor(executor, fn, *args))

    """
    # TODO: Generalize and move to scalems.execution
    import scalems.execution

    if (_current_executor := scalems.execution.current_executor.get(default=None)) is not None:
        raise scalems.exceptions.ProtocolError(f"Executor {_current_executor} is already active.")
    _current_executor: scalems.execution.ScalemsExecutorT = await provision_executor(
        runtime_context, worker_requirements=worker_requirements, task_requirements=task_requirements
    )

    # TODO: Integrate the queue manager.
    # # Avoid race conditions while checking for a running dispatcher.
    # # TODO: Clarify dispatcher state machine and remove/replace assertions.
    # # Warning: The dispatching protocol is immature.
    # # Initially, we don't expect contention for the lock,
    # # and if there is contention, it probably represents
    # # an unintended race condition or systematic dead-lock.
    # assert not self._dispatcher_lock.locked()
    # async with self._dispatcher_lock:
    #     # Dispatching state may be reentrant, but it does not make sense to
    #     # re-enter through this call structure.
    #     if self._dispatcher is not None:
    #         raise ProtocolError(f"Already dispatching through {repr(self._dispatcher)}.")
    #
    #     dispatcher: "Queuer" = None
    #     if dispatcher is None:
    #         dispatcher = Queuer(source=self, command_queue=executor.queue(), dispatcher_lock=self._dispatcher_lock)
    #         self._dispatcher = dispatcher
    #     else:
    #         self._dispatcher = weakref.proxy(dispatcher)

    runtime_session: RuntimeSession = runtime_context.runtime_session
    assert runtime_session.raptor is None

    token = scalems.execution.current_executor.set(_current_executor)
    assert token.old_value is contextvars.Token.MISSING
    try:
        yield _current_executor
    finally:
        # Note: copies of the Context will retain references to the executor.
        assert scalems.execution.current_executor.get() == _current_executor
        scalems.execution.current_executor.reset(token)
        _current_executor.shutdown(wait=False, cancel_futures=False)
        # Note: This means that `shutdown` may have already run before Tasks have completed.
        # How do we allow Tasks to submit new subtasks while executing? What does `shutdown`
        # mean if we allow that?
        # We could say that `shutdown` means `submit` may not be called again for _this_
        # executor. We will have a different Executor implementation where tasks are actually
        # executed, and we can document that this `executor_scope` version of the
        # protocol needs to be avoided or used carefully when tasks need to submit tasks.
