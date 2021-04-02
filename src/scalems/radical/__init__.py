"""Workflow subpackage for ScaleMS execution dispatching to RADICAL Pilot.

Example:
    python -m scalems.radical myworkflow.py

Manage workflow context for RADICAL Pilot.

Dispatching through RADICAL Pilot is still evolving, and this
module may provide multiple disparate concepts.

Workflow Manager:
    RPWorkflowContext provides a SCALE-MS workflow context and coordinates
    resources for a RADICAL Pilot Session.

Executor:
    The RP dispatcher and executor are currently combined, and provided only
    as the implementation of the `context.dispatch` member function.

    When "entered" (i.e. used as
    a :py:func:`with`), the Python Context Manager protocol manages the
    lifetime of a radical.pilot.Session. Two significant areas of future
    development include Context chaining, and improved support for multiple rp.Sessions
    through multiple RPContextManager instances.

"""
# TODO: Consider converting to a namespace package to improve modularity of implementation.

import asyncio
import concurrent
import contextlib
import copy
import dataclasses
import functools
import inspect
import json
import logging
import os
import queue
# Note that we import RP early to allow it to monkey-patch some modules early.
import tempfile
import threading
import typing
import warnings
from concurrent.futures import Future
from typing import Any

import packaging.version
from radical import pilot as rp
from scalems.exceptions import APIError

import scalems.context
from scalems.exceptions import DispatchError
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.exceptions import ScaleMSError

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


class QueueItem(typing.Dict[str, Any]):
    """Queue items are either workflow items or control messages."""

    def _hexify(self):
        for key, value in self.items():
            if isinstance(value, bytes):
                value = value.hex()
            yield key, value

    def __str__(self) -> str:
        return str(dict(self._hexify()))


@dataclasses.dataclass
class RPParams:
    execution_target: str = 'local.localhost'
    rp_resource_params: dict = dataclasses.field(default_factory=dict)
    target_venv: str = None


class RPWorkflowContext(scalems.context.WorkflowManager):
    """Manage a workflow context for RADICAL Pilot work loads.

    The rp.Session is created when the Python Context Manager is "entered",
    so the asyncio event loop must be running before then.

    To help enforce this, we use an async Context Manager, at least in the
    initial implementation. However, the implementation is not thread-safe.
    It is not reentrant, but this is not checked. We probably _do_ want to
    constrain ourselves to zero or one Sessions per environment, but we _do_
    need to support multiple Pilots and task submission scopes (_resource
    requirement groups).
    Further discussion is welcome.

    Warning:
        The importer of this module should be sure to import radical.pilot
        before importing the built-in logging module to avoid spurious warnings.
    """

    def __init__(self, loop):
        super(RPWorkflowContext, self).__init__(loop)

    @contextlib.asynccontextmanager
    async def dispatch(self):
        """Enter the execution dispatching state.

        Attach to a dispatching executor, then provide a scope for concurrent activity.
        This is also the scope during which the RADICAL Pilot Session exists.

        Provide the executor with any currently-managed work in a queue.
        While the context manager is active, new work added to the queue will be picked up
        by the executor. When the context manager is exited, new work will resume
        queuing locally and the remote tasks will be resolved, then the dispatcher
        will be disconnected.

        Currently, we tie the lifetime of the dispatcher to this context manager.
        When leaving the `with` block, we trigger the executor to clean-up and wait for its task to complete.
        We may choose some other relationship in the future.

        .. todo:: Clarify re-entrance policy, thread-safety, etcetera, and enforce.

        .. todo:: Allow an externally provided dispatcher factory, or even a running dispatcher?

        """

        # 1. Install a hook to catch new calls to add_item (the dispatcher_queue)
        #    and try not to yield until the current workflow state is obtained.
        # 2. Get snapshot of current workflow state with which to initialize the dispatcher. (It is now okay to yield.)
        # 3. Bind a new executor to its queue.
        # 4. Bind a dispatcher to the executor and the dispatcher_queue.
        # 5. Allow the executor and dispatcher to start using the event loop.

        # Avoid race conditions while checking for a running dispatcher.
        async with self._dispatcher_lock:
            # Dispatching state may be reentrant, but it does not make sense to re-enter through this call structure.
            if self._dispatcher is not None:
                raise ProtocolError('Already dispatching through {}.'.format(repr(self._dispatcher())))
            # For an externally-provided dispatcher:
            #     else:
            #         self._dispatcher = weakref.ref(dispatcher)

            # 1. Install a hook to catch new calls to add_item
            if self._queue is not None:
                raise ProtocolError('Found unexpected dispatcher queue.')
            dispatcher_queue = queue.SimpleQueue()
            self._queue = dispatcher_queue

            # 2. Get snapshot of current workflow state with which to initialize the dispatcher.
            # TODO: Topologically sort DAG!
            initial_task_list = list(self.task_map.keys())
            #  It is now okay to yield at the queue.put(), but we hold the _dispatcher_lock
            #  until the dispatcher and executor are running.

            # 3. Bind a new executor to its queue.
            # Note: if there were a reason to decouple the executor lifetime from this scope,
            # we could consider a more object-oriented interface with it.
            executor_queue = asyncio.Queue()
            for _task_id in initial_task_list:
                await executor_queue.put(QueueItem({'add_item': _task_id}))

            execution_target = getattr(self, '_rp_execution_target', 'local.localhost')
            rp_resource_params = getattr(self, '_rp_resource_params', {})

            # Note: the executor owns a rp.Session during operation. Manage scope
            # of executor operation with a context manager. RP does not yet use an event loop,
            # but we can use async context manager for future compatibility with asyncio
            # management of network connections, etc.
            # TODO: Provide the handles to the relevant locks.
            executor = RPDispatchingExecutor(source_context=self,
                                             loop=self._asyncio_event_loop,
                                             command_queue=executor_queue,
                                             rp_params=RPParams(
                                                 execution_target=execution_target,
                                                 rp_resource_params=rp_resource_params,
                                                 target_venv='/home/rp/rp-venv'
                                             ),
                                             dispatcher_lock=self._dispatcher_lock,
                                             )

            # 4. Bind a dispatcher to the executor_queue and the dispatcher_queue.
            # TODO: We should bind the dispatcher directly to the executor, but that requires
            #  that we make an Executor class with concurrency-safe methods.
            # dispatcher = run_dispatcher(dispatcher_queue, executor_queue)
            # self._dispatcher = weakref.ref(dispatcher)
            # TODO: Toggle active dispatcher state.
            # scalems.context._dispatcher.set(...)

            # 5. Allow the executor and dispatcher to start using the event loop.
            assert not asyncio.iscoroutine(executor)
            assert not asyncio.isfuture(executor)
            assert inspect.isawaitable(executor)
            executor_task = asyncio.ensure_future(executor)
            # asyncio.create_task(dispatcher)

        try:
            # We can surrender control here and leave the executor and dispatcher tasks active
            # while evaluating a `with` block suite for the `dispatch` context manager.
            async with executor as dispatching_session:
                yield dispatching_session

        except Exception as e:
            logger.exception('Uncaught exception while in dispatching context: {}'.format(str(e)))
            raise e

        finally:
            try:
                executor_result = await executor_task
            except Exception as e:
                logger.exception('Executor task finished with exception', exc_info=True)
                executor_result = None
            assert executor_task.done()

            async with self._dispatcher_lock:
                self._dispatcher = None
                self._queue = None
            # dispatcher_queue.put({'control': 'stop'})
            # await dispatcher
            # TODO: Make sure the dispatcher hasn't died. Look for acknowledgement
            #  of receipt of the Stop command.
            # TODO: Check status...
            if not dispatcher_queue.empty():
                logger.error('Dispatcher finished while items remain in dispatcher queue. Approximate size: {}'.format(
                    dispatcher_queue.qsize()))

            if not executor_task.cancelled() and not executor_task.exception():
                # Check that the queue drained.
                # WARNING: The queue will never finish draining if executor_task fails.
                #  I.e. don't `await executor_queue.join()`
                if not executor_queue.empty():
                    # TODO: Handle non-empty queue.
                    # There are various reasons that the queue might not be empty and we should
                    # clean up properly instead of bailing out or compounding exceptions.
                    logger.error('Bug: Executor left tasks in the queue without raising an exception.')

            logger.debug('Exiting {} dispatch context.'.format(type(self).__name__))

            # if loop.is_running():
            #     # Clean up unawaited tasks.
            #     loop.run_until_complete(loop.shutdown_asyncgens())
            #     # Do we need to check the work graph directly?


class RPResult:
    """Basic result type for RADICAL Pilot tasks.

    Define a return type for Futures or awaitable tasks from
    RADICAL Pilot commands.
    """
    # TODO: Provide support for RP-specific versions of standard SCALEMS result types.


class RPTaskFailure(ScaleMSError):
    """Error in radical.pilot.Task execution.

    Attributes:
        failed_task: A dictionary representation of the failed task.

    TODO: What can/should we capture and report from the failed task?
    """
    failed_task: dict

    def __init__(self, *args, task: rp.Task):
        super().__init__(*args)
        self.failed_task = task.as_dict()


class RPFinalTaskState:
    def __init__(self):
        self.canceled = threading.Event()
        self.done = threading.Event()
        self.failed = threading.Event()

    def __bool__(self):
        return self.canceled.is_set() or self.done.is_set() or self.failed.is_set()


def _rp_callback(obj: rp.Task, state, final: RPFinalTaskState):
    """Prototype for RP.Task callback.

    To use, partially bind the *final* parameter (with functools.partial) to get a callable
    with the RP.Task callback signature.

    Register with *task* to be called when the rp.Task state changes.
    """
    logger.debug(f'Callback triggered by {repr(obj)} state change to {repr(state)}.')
    try:
        # Note: assertions and exceptions are not useful in RP callbacks.
        # TODO: Can/should we register the call-back just for specific states?
        if state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED):
            # TODO: unregister call-back with RP Task or redirect subsequent call-backs.
            if state == rp.states.DONE:
                final.done.set()
            elif state == rp.states.CANCELED:
                final.canceled.set()
            elif state == rp.states.FAILED:
                final.failed.set()
            else:
                logger.error('Bug: logic error in state cases.')
    except Exception as e:
        logger.error(f'Exception encountered during rp.Task callback: {repr(e)}')


async def _rp_task_watcher(task: rp.Task, future: asyncio.Future, final: RPFinalTaskState, ready: asyncio.Event):
    """Manage the relationship between an RP.Task and a scalems Future.

    Cancel the RP.Task if this task or the scalems.Future is canceled.

    Publish the RP.Task result or cancel the scalems.Future if the RP.Task is
    done or canceled.

    Arguments:
        ready: output parameter, set when coroutine has run enough to perform its responsibilities.

    Caller should await the *ready* event before assuming the watcher task is doing its job.

    .. todo:: See asyncio.wrap_future and asyncio.run_coroutine_threadsafe
    """
    try:
        ready.set()
        finished = lambda: task.state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED) \
                           or future.done() \
                           or final
        while not finished():
            # Let the watcher wake up periodically to check for state changes.
            # TODO: Use a control thread to manage *threading* primitives and translate to asyncio primitives.
            done, pending = await asyncio.wait([future], timeout=0.1, return_when=asyncio.FIRST_COMPLETED)
            if future.cancelled():
                if task.state != rp.states.CANCELED:
                    task.cancel()
                logger.debug('Propagating cancellation from scalems futur to rp task.')
                return task
            if final:
                logger.debug(f'Handling finalization for RP task {task.uid}')
                if final.failed.is_set():
                    if not future.cancelled():
                        assert not future.done()
                        logger.debug('Propagating RP Task failure.')
                        # TODO: Provide more useful error feedback.
                        future.set_exception(RPTaskFailure(f'{task.uid} failed.', task=task))
                elif final.canceled.is_set():
                    logger.debug('Propagating RP Task cancellation to scalems future.')
                    future.cancel()
                    raise asyncio.CancelledError("Managed RP.Task was cancelled.")
                else:
                    assert final.done.is_set()
                    if not future.cancelled():
                        logger.debug('Publishing RP Task result to scalems Future.')
                        # TODO: Manage result type better.
                        future.set_result(task.as_dict())
                return task
            if task.state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED):
                if not final:
                    logger.debug(f'RP Task {task.uid} complete, but Event not triggered. Possible race condition.')
    except asyncio.CancelledError as e:
        logger.debug('Propagating scalems manager task cancellation to scalems future and rp task.')
        future.cancel()
        task.cancel()
        raise e


async def rp_task(rptask: rp.Task, future: asyncio.Future) -> asyncio.Task:
    """Mediate between a radical.pilot.Task and an asyncio.Future.

    Schedule an asyncio Task to receive the result of the RP Task. The asyncio
    Task must also make sure that asyncio cancellation propagates to the rp.Task.cancel,
    and vice versa.

    This function should be awaited immediately to make sure the necessary call-backs
    get registered. The result will be an asyncio.Task, which should be awaited
    separately.

    Internally, this function provides a call-back to the rp.Task. The call-back
    provided to RP cannot directly call asyncio.Future methods (such as set_result() or set_exception())
    because RP will be making the call from another thread without mediation by the
    asyncio event loop.

    As such, we also need to provide a thread-safe event handler to propagate the
    RP Task call-back to the asyncio Future.

    Canceling the returned task will cause both *rptask* and *future* to be canceled.
    Canceling *rptask* will cause this task and *future* to be canceled.
    Canceling *future* will cause *rptask* to be canceled, but will not cancel this task.

    Arguments:
        rptask: RADICAL Pilot Task that has already been submitted.
        future: Future to which rptask results will be published.

    Returns:
        A Task that, when awaited, returns the rp.Task instance in its final state.
    """
    if not isinstance(rptask, rp.Task):
        raise TypeError('Function requires a RADICAL Pilot Task object.')
    _rp_task = rptask

    final = RPFinalTaskState()
    callback = functools.partial(_rp_callback, final=final)
    functools.update_wrapper(callback, _rp_callback)
    rptask.register_callback(callback)

    watcher_started = asyncio.Event()
    wrapped_task = asyncio.create_task(_rp_task_watcher(task=rptask, future=future, final=final, ready=watcher_started))
    # Make sure that the task is cancellable before returning it to the caller.
    await watcher_started.wait()
    # watcher_task.
    return wrapped_task


def _describe_task(item: scalems.context.Task, scheduler: str) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    The TaskDescription will be submitted to the named *scheduler*,
    where *scheduler* is the UID of a task managing the life of a rp.raptor.Master instance.

    Caller is responsible for ensuring that *scheduler* is valid.
    """
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    # Ref: scalems_rp_agent._RaptorTaskDescription
    task_description = rp.TaskDescription(
        from_dict=dict(
            executable='scalems'
        )
    )
    task_description.uid = item.uid()
    task_description.scheduler = str(scheduler)

    task_description.arguments = [item.serialize()]

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    return task_description


async def submit_rp_task(item: scalems.context.Task, task_manager: rp.TaskManager, submitted: asyncio.Event = None, scheduler: str = None):
    """Dispatch a WorkflowItem to be handled by RADICAL Pilot.

    Registers a Future for the task result with *item*.

    Args:
        item: The workflow item to be submitted
        task_manager: A radical.pilot.TaskManager instance through which the task should be submitted.
        submitted: (Output parameter) Caller-provided Event to be set once the RP Task has been submitted.
        scheduler (str): The string name of the "scheduler," corresponding to the UID of a Task running a rp.raptor.Master.

    Returns an asyncio.Task for a submitted rp.Task.

    The *submitted* (output) event is likely a short-term placeholder and subject to change.
    For instance, the use case for waiting on such an event could be met by waiting on the
    state change of the workflow item to a SUBMITTED state. However, note that this function
    will block for a short time at the rp.TaskManager.submit_tasks() call, so it is useful
    to separate the submission event from the completion of this coroutine early in development
    while we decide whether and how to relegate RP calls to threads separated from that of
    the event loop.

    The returned asyncio.Task can be used to cancel the rp.Task (and the Future)
    or to await the RP.Task cleanup.

    To submit tasks as a batch, await an array of submit_rp_task() results in the
    same dispatching context. (TBD)

    Notes:

        workflow manager maintains the workflow state without expensive or stateful volatile resources,
        and can mediate updates to the managed workflow at any time. Items enter the graph in an IDLE state. The
        WorkflowManager can provide Futures for the results of the managed items. For IDLE items, the WorkflowManager
        retains a weakref to the issued Futures, which it can use to make sure that there is only zero or one Future for
        a particular result.

        WorkflowManager collaborates with Dispatcher to transition the graph to an "active" or "executing" state.
        This transition is mediated through the dispatcher_lock.

        Dispatcher sequences and queues workflow items to be handled, pushing them to a dispatch_queue. No state
        change to the workflow item seems necessary at this time.

        The dispatch_queue is read by an ExecutionManager. Items may be processed immediately or staged in a
        command_queue. Workflow items are then either SUBMITTED or BLOCKED (awaiting dependencies). Optionally,
        Items may be marked ELIGIBLE and re-queued for batch submission.

        If the ExecutionManager is able to submit a task, the Task has a call-back registered for the workflow item.
        The WorkflowManager needs to convert any Future weakrefs to strong references when items are SUBMITTED,
        and the workflow Futures are subscribed to the item. Tasks are wrapped in a scalems object that the
        WorkflowManager is able to take ownership of. BLOCKED items are wrapped in Tasks which are subscribed to
        their dependencies (WorkflowItems should already be subscribed to WorkflowItem Futures for any dependencies)
        and stored by the ExecutionManager. When the call-backs for all of the dependencies indicate the Item should
        be processed into an upcoming workload, the Item becomes ELIGIBLE, and its wrapper Task (in collaboration
        with the ExecutionManager) puts it in the command_queue.

        As an optimization, and to support co-scheduling, a WorkflowItem call-back can provide notification of state
        changes. For instance, a BLOCKED item may become ELIGIBLE once all of its dependencies are SUBMITTED,
        when the actual Executor has some degree of data flow management capabilities.

    """
    if isinstance(scheduler, str) and len(scheduler) > 0 and isinstance(task_manager.get_tasks(scheduler), rp.Task):
        # We might want a contextvars.Context to hold the current rp.Master instance name.
        rp_task_description = _describe_task(item, scheduler)
    else:
        raise APIError('Caller must provide the UID of a submitted *scheduler* task.')

    loop = asyncio.get_running_loop()
    rp_task_result_future = loop.create_future()

    # Warning: in the long run, we should not extend the life of the reference returned
    # by edit_item, and we need to consider the robust way to publish item results.
    # TODO: Translate RP result to item result type.
    rp_task_result_future.add_done_callback(item.set_result)

    # TODO: Move slow blocking RP calls to a separate RP control thread.
    rp_task = task_manager.submit_tasks(rp_task_description)
    # TODO: What checks can/should we do to allow checks that dependencies have been successfully scheduled?
    if isinstance(submitted, asyncio.Event):
        submitted.set()

    rp_task_watcher = await rp_task(rptask=rp_task, future=rp_task_result_future)

    return rp_task_watcher


class RPDispatchingExecutor:
    """Client side manager for work dispatched through RADICAL Pilot.

    Configuration points::
    * resource config
    * pilot config
    * session config?
    """
    command_queue: asyncio.Queue
    execution_target: str
    scheduler: typing.Union[rp.Task, None]
    session: typing.Union[rp.Session, None]
    source_context: scalems.context.WorkflowManager
    rp_tasks: typing.List[asyncio.Task]

    _dispatcher_lock: asyncio.Lock
    _pilot_description: rp.PilotDescription
    _rp_resource_params: typing.Optional[dict]
    _task_description: rp.TaskDescription
    _queue_runner_task: asyncio.Task

    async def __aenter__(self):
        try:
            # Get a lock while the state is changing.
            async with self._dispatcher_lock:
                self._connect_rp()
                if self.session is None or self.session.closed:
                    raise ProtocolError('Cannot process queue without a RP Session.')

                # Launch queue processor (proxy executor).
                runner_started = asyncio.Event()
                task_manager = self.session.get_task_managers(tmgr_uids=self._task_manager_uid)
                runner_task = asyncio.create_task(self._queue_runner(task_manager, runner_started))
                await runner_started.wait()
                self._queue_runner_task = runner_task

            # Note: it would probably be most useful to return something with a WorkflowManager
            # interface...
            return self
        except Exception as e:
            self._exception = e
            raise e

    def _connect_rp(self):
        """Establish the RP Session.

        Acquire as many re-usable resources as possible. The scope established by
        this function is as broad as it can be within the life of this instance.

        Once instance._connect_rp() succeeds, instance._disconnect_rp() must be called to clean
        up resources. Use the async context manager behavior of the instance to automatically
        follow this protocol. I.e. instead of calling ``instance._connect_rp(); ...; instance._disconnect_rp()``,
        use::
            async with instance:
                ...

        Raises:
            DispatchError if task dispatching could not be set up.

            CanceledError if parent asyncio.Task is cancelled while executing.

        """
        # TODO: Consider inlining this into __aenter__().
        # A non-async method is potentially useful for debugging, but causes the event loop to block
        # while waiting for the RP tasks included here. If this continues to be a slow function,
        # we can wrap the remaining RP calls and let this function be inlined, or stick the whole
        # function in a separate thread with loop.run_in_executor().

        # TODO: RP triggers SIGINT in various failure modes. We should use loop.add_signal_handler() to convert to an exception
        #       that we can raise in an appropriate task.
        # Note that PilotDescription can use `'exit_on_error': False` to suppress the SIGINT,
        # but we have not explored the consequences of doing so.

        try:
            #
            # Start the Session.
            #

            # Note that we cannot resolve the full _resource config until we have a Session object.
            # We cannot get the default session config until after creating the Session,
            # so we don't have a template for allowed, required, or default values.
            # Question: does the provided *cfg* need to be complete? Or will it be merged
            # with default values from some internal definition, such as by dict.update()?
            # I don't remember what the use cases are for overriding the default session config.
            session_config = None
            # At some point soon, we need to track Session ID for the workflow metadata.
            # We may also want Session ID to be deterministic (or to be re-used?).
            session_id = None

            # Note: the current implementation implies that only one Task for the dispatcher
            # will exist at a time. We are further assuming that there will probably only
            # be one Task per the lifetime of the dispatcher object.
            # We could choose another approach and change our assumptions, if appropriate.
            if self.session is not None:
                raise ProtocolError('Dispatching context is not reentrant.')
            logger.debug('Entering RP dispatching context. Waiting for rp.Session.')

            # This would be a good time to `await`, if an event-loop friendly Session creation function becomes available.
            self.session = rp.Session(uid=session_id, cfg=session_config)
            session_id = self.session.uid
            session_config = copy.deepcopy(self.session.cfg.as_dict())
            logger.debug('RP dispatcher acquired session {}'.format(session_id))

            # We can launch an initial Pilot, but we may have to run further Pilots
            # during self._queue_runner_task (or while servicing scalems.wait() within the with block)
            # to handle dynamic work load requirements.
            # Optionally, we could refrain from launching the pilot here, at all,
            # but it seems like a good chance to start bootstrapping the agent environment.
            logger.debug('Launching PilotManager.')
            pilot_manager = rp.PilotManager(session=self.session)
            self._pilot_manager_uid = pilot_manager.uid
            logger.debug('Got PilotManager {}.'.format(self._pilot_manager_uid))

            logger.debug('Launching TaskManager.')
            task_manager = rp.TaskManager(session=self.session)
            self._task_manager_uid = task_manager.uid
            logger.debug(('Got TaskManager {}'.format(self._task_manager_uid)))

            #
            # Get a Pilot
            #

            # # TODO: Describe (link to) configuration points.
            # resource_config['local.localhost'].update({
            #     'project': None,
            #     'queue': None,
            #     'schema': None,
            #     'cores': 1,
            #     'gpus': 0
            # })

            # _pilot_description = dict(_resource=_resource,
            #                          runtime=30,
            #                          exit_on_error=True,
            #                          project=resource_config[_resource]['project'],
            #                          queue=resource_config[_resource]['queue'],
            #                          cores=resource_config[_resource]['cores'],
            #                          gpus=resource_config[_resource]['gpus'])

            # TODO: How to specify PilotDescription?
            # Where should this actually be coming from?
            # We need to inspect both the HPC allocation and the work load, I think,
            # and combine with user-provided preferences.
            self._pilot_description = rp.PilotDescription(
                {'resource': self.execution_target,
                 'cores': 4,
                 'gpus': 0})

            # How and when should we update pilot description?
            logger.debug('Submitting PilotDescription {}'.format(repr(self._pilot_description)))
            pilot = pilot_manager.submit_pilots(self._pilot_description)
            self.pilot_uid = pilot.uid
            logger.debug('Got Pilot {}'.format(pilot.uid))

            # Note that the task description for the master (and worker) can specify a *named_env* attribute to use
            # a venv prepared via Pilot.prepare_env
            # E.g.         pilot.prepare_env({'numpy_env' : {'type'   : 'virtualenv',
            #                                           'version': '3.6',
            #                                           'setup'  : ['numpy']}})
            #   td.named_env = 'numpy_env'
            # Note that td.named_env MUST be a key that is given to pilot.prepare_env(arg: dict) or
            # the task will wait indefinitely to be scheduled.
            # Alternatively, we could use a pre-installed venv by putting `. path/to/ve/bin/activate`
            # in the TaskDescription.pre_exec list.

            # TODO: Use archives generated from (acquired through) the local installations.
            # # Could we stage in archive distributions directly?
            # # self.pilot.stage_in()
            # rp_spec = 'radical.pilot@git+https://github.com/radical-cybertools/radical.pilot.git@project/scalems'
            # rp_spec = shlex.quote(rp_spec)
            # scalems_spec = shlex.quote('scalems@git+https://github.com/SCALE-MS/scale-ms.git@sms-54')
            # pilot.prepare_env(
            #     {
            #         'scalems_env': {
            #             'type': 'virtualenv',
            #             'version': '3.8',
            #             'setup': [
            #                 # TODO: Generalize scalems dependency resolution.
            #                 # Ideally, we would check the current API version requirement, map that to a package version,
            #                 # and specify >=min_version, allowing cached archives to satisfy the dependency.
            #                 rp_spec,
            #                 scalems_spec
            #             ]}})

            # Question: when should we remove the pilot from the task manager?
            task_manager.add_pilots(pilot)
            logger.debug('Added Pilot {} to task manager {}.'.format(self.pilot_uid, self._task_manager_uid))

            # pilot_sandbox = urlparse(self.pilot.pilot_sandbox).path
            #
            # # I can't figure out another way to avoid the Pilot venv...
            # py_base = '/usr/bin/python3'
            #
            # venv = os.path.join(pilot_sandbox, 'scalems_venv')
            # task = self._task_manager.submit_tasks(
            #     rp.TaskDescription(
            #         {
            #             'executable': py_base,
            #             'arguments': ['-m', 'venv', venv],
            #             'environment': {}
            #         }
            #     )
            # )
            # if self._task_manager.wait_tasks(uids=[task.uid])[0] != rp.states.DONE or task.exit_code != 0:
            #     raise DispatchError('Could not create venv.')
            #
            # py_venv = os.path.join(venv, 'bin', 'python3')
            # task = self._task_manager.submit_tasks(
            #     rp.TaskDescription(
            #         {
            #             'executable': py_venv,
            #             'arguments': ['-m', 'pip', 'install', '--upgrade',
            #                           'pip',
            #                           'setuptools',
            #                           # It seems like the install_requires may not get followed correctly,
            #                           # or may be unsatisfied without causing this task to fail...
            #                           shlex.quote('radical.pilot@git+https://github.com/radical-cybertools/radical.pilot.git@project/scalems'),
            #                           shlex.quote('scalems@git+https://github.com/SCALE-MS/scale-ms.git@sms-54')
            #                           ]
            #         }
            #     )
            # )
            # if self._task_manager.wait_tasks(uids=[task.uid])[0] != rp.states.DONE or task.exit_code != 0:
            #     raise DispatchError('Could not install execution environment.')

            # Verify usable SCALEMS RP connector.
            # TODO: Fetch a profile of the venv for client-side analysis (e.g. `pip freeze`).
            # TODO: Check for compatible installed scalems API version.
            rp_check = task_manager.submit_tasks(
                rp.TaskDescription(
                    {
                        # 'executable': py_venv,
                        'executable': 'python3',
                        'arguments': ['-c', 'import radical.pilot as rp; print(rp.version)'],
                        'pre_exec': self._pre_exec
                        # 'named_env': 'scalems_env'
                    }
                )
            )
            logger.debug('Checking RP execution environment.')
            if task_manager.wait_tasks(uids=[rp_check.uid])[0] != rp.states.DONE or rp_check.exit_code != 0:
                raise DispatchError('Could not verify RP in execution environment.')

            try:
                remote_rp_version = packaging.version.parse(rp_check.stdout.rstrip())
            except:
                remote_rp_version = None
            if not remote_rp_version or remote_rp_version < packaging.version.parse('1.6.0'):
                raise DispatchError('Could not get a valid execution environment.')

            #
            # Get a scheduler task.
            #

            # TODO: encapsulate in a function that we can run in a separate rp control thread.
            from scalems.radical import scalems_rp_agent

            _activate_venv: str = '. ' + os.path.join(self._target_venv, 'bin', 'activate')
            scheduler_config = scalems_rp_agent.SchedulerConfig(
                worker_descr=scalems_rp_agent.WorkerDescription(
                    pre_exec=[_activate_venv]
                ))

            # This is the name that should be resolvable in an active venv for the script we install
            # as pkg_resources.get_entry_info('scalems', 'console_scripts', 'scalems_rp_agent').name
            master_script = 'scalems_rp_agent'

            scheduler_name = 'raptor.scalems'
            # We can probably make the config file a permanent part of the local metadata,
            # but we don't really have a scheme for managing local metadata right now.
            with tempfile.TemporaryDirectory() as dir:
                config_file_name = 'raptor_scheduler_config.json'
                config_file_path = os.path.join(dir, config_file_name)
                with open(config_file_path, 'w') as fh:
                    encoded = scalems_rp_agent.encode_as_dict(scheduler_config)
                    json.dump(encoded, fh, indent=2)

                # define a raptor.scalems master and launch it within the pilot
                td = rp.TaskDescription(
                    {
                        'uid': scheduler_name,
                        'executable': master_script})
                td.arguments = [config_file_name]
                td.pre_exec = [_activate_venv]
                td.input_staging = {
                    'source': config_file_path,
                    'target': config_file_name,
                    'action': rp.TRANSFER
                }
                # td.named_env = 'scalems_env'
                logger.debug('Launching RP scheduler.')
                scheduler = task_manager.submit_tasks(td)
                # Wait for the state after TMGR_STAGING_INPUT
                # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not check type.
                scheduler.wait(state=[rp.states.AGENT_STAGING_INPUT_PENDING])
            # end TODO
            assert self.scheduler is None
            self.scheduler = scheduler
            # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
            # Note: The worker script name only appears in the config file.

        except asyncio.CancelledError as e:
            raise e
        except Exception as e:
            raise DispatchError('Failed to launch SCALE-MS master task.') from e

    async def _queue_runner(self, task_manager: rp.TaskManager, processing_state: asyncio.Event):
        processing_state.set()
        # Note that if an exception is raised here, the queue will never be processed.
        while True:
            # Note: If the function exits at this line, the queue may be missing a call
            #  to task_done() and should not be `join()`ed. It does not seem completely
            #  clear whether the `get()` will have completed if the task for this function
            #  is canceled or a Task.send() or Task.throw() is used. Alternatively, we could
            #  include the `get()` in the `try` block and catch a possible ValueError at the
            #  task_done() call, in case we arrive there without Queue.get() having completed.
            #  However, that could allow a Queue.join() to complete early by accident.
            command: QueueItem = await self.command_queue.get()
            try:
                # TODO: Use formal RPC protocol.
                if 'control' in command:
                    if command['control'] == 'stop':
                        # This effectively breaks the `while True` loop, but may not be obvious.
                        # Consider explicit `break` to clarify that we want to run off the end
                        # of the function.
                        return
                    else:
                        raise ProtocolError('Unknown command: {}'.format(command['control']))
                else:
                    if 'add_item' not in command:
                        # TODO: We might want a call-back or Event to force errors before the queue-runner task is awaited.
                        raise MissingImplementationError('Executor has no implementation for {}'.format(str(command)))

                key = command['add_item']
                with self.source_context.edit_item(key) as item:
                    if not isinstance(item, scalems.context.Task):
                        raise InternalError(
                            'Bug: Expected {}.edit_item() to return a scalems.context.Task'.format(repr(self.source_context)))

                    # TODO: Ensemble handling
                    item_shape = item.description().shape()
                    if len(item_shape) != 1 or item_shape[0] != 1:
                        raise MissingImplementationError('Executor cannot handle multidimensional tasks yet.')

                    # Bind the WorkflowManager item to an RP Task.

                    # TODO: Optimization: skip tasks that are already done.
                    # TODO: Check task dependencies.
                    submitted = asyncio.Event()  # Not yet used.
                    rp_task = submit_rp_task(item,
                                             task_manager=task_manager,
                                             submitted=submitted,
                                             scheduler=self.scheduler.uid)

                    self.rp_tasks.append(rp_task)

                    # This is the role of the Executor, not the Dispatcher.
                    # task_type: scalems.context.ResourceType = item.description().type()
                    # # Note that we could insert resource management here. We could create
                    # # tasks until we run out of free resources, then switch modes to awaiting
                    # # tasks until resources become available, then switch back to placing tasks.
                    # execution_context = _ExecutionContext(source_context, key)
                    # if execution_context.done():
                    #     if isinstance(key, bytes):
                    #         id = key.hex()
                    #     else:
                    #         id = str(key)
                    #     logger.info(f'Skipping task that is already done. ({id})')
                    #     # TODO: Update local status and results.
                    # else:
                    #     assert not item.done()
                    #     assert not source_context.item(key).done()
                    #     await _execute_item(task_type=task_type,
                    #                         item=item,
                    #                         execution_context=execution_context)

            finally:
                # Warning: there is a tiny chance that we could receive a asyncio.CancelledError at this line
                # and fail to decrement the queue.
                logger.debug('Releasing "{}" from command queue.'.format(str(command)))
                self.command_queue.task_done()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up at context exit.

        In addition to handling exceptions, the main resource to clean up is the rp.Session.

        We also need to make sure that we properly disengage from any queues or generators.

        We can also leave flags for ourself to be checked at __await__, if there is a Task
        associated with the Executor.
        """
        # Note that this coroutine could take a long time and could be cancelled at several points.
        cancelled_error = None
        async with self._dispatcher_lock:
            session: rp.Session = self.session
            # This method is not thread safe, but we try to make clear that instance.session
            # is no longer publicly available as soon as possible.
            self.session = None

            if session is None or session.closed:
                logger.error('rp.Session is already closed?!')
            else:
                try:
                    # Stop the executor.
                    logger.debug('Stopping the SCALEMS RP dispatching executor.')
                    # TODO: Make sure that nothing else will be adding items to the queue from this point.
                    # We should establish some assurance that the next line represents the last thing
                    # that we will put in the queue.
                    self.command_queue.put_nowait({'control': 'stop'})
                    # TODO: Consider what to do differently when we want to cancel work rather than just finalize it.

                    # done, pending = await asyncio.wait(aws, timeout=0.1, return_when=FIRST_EXCEPTION)
                    # Currently, the queue runner does not place subtasks, so there is only one thing to await.
                    # TODO: We should probably allow the user to provide some sort of timeout, or infer one from other time limits.
                    try:
                        if not self._queue_runner_task.done():
                            await self._queue_runner_task
                    except asyncio.CancelledError as e:
                        raise e
                    except Exception as queue_runner_exception:
                        logger.exception('Unhandled exception when stopping queue handler.', exc_info=True)
                        self._exception = queue_runner_exception
                    else:
                        logger.debug('Queue runner task completed.')
                    finally:
                        if not self.command_queue.empty():
                            logger.error('Command queue never emptied.')

                        # This should be encapsulated into the task watcher and just gathered.
                        # Do we need to wait for the RP Tasks?
                        # task_manager = session.get_task_managers(tmgr_uids=self._task_manager_uid)
                        # if len(self.rp_tasks) > 0:
                        #     logger.debug('Waiting for RP tasks: {}'.format(', '.join([t.uid for t in self.rp_tasks])))
                        #     states = task_manager.wait_tasks(uids=[t.uid for t in self.rp_tasks])
                        #     assert states
                        #     logger.debug('Worker tasks complete.')
                        #     for t in self.rp_tasks:
                        #         logger.info('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
                        #         if t.state != rp.states.DONE or t.exit_code != 0:
                        #             logger.error(f'RP Task unsuccessful: {repr(t)}')
                        # end annotation

                        # Wait for the watchers.
                        if len(self.rp_tasks) > 0:
                            results = await asyncio.gather(self.rp_tasks)

                        # Cancel the master.
                        logger.debug('Canceling the master scheduling task.')
                        task_manager = session.get_task_managers(tmgr_uids=self._task_manager_uid)
                        task_manager.cancel_tasks(uids=self.scheduler.uid)
                        # Cancel blocks until the task is done so the following wait would (currently) be redundant,
                        # but there is a ticket open to change this behavior.
                        # See https://github.com/radical-cybertools/radical.pilot/issues/2336
                        # tmgr.wait_tasks([scheduler.uid])
                        logger.debug('Master scheduling task complete.')

                        pmgr: rp.PilotManager = session.get_pilot_managers(pmgr_uids=self._pilot_manager_uid)
                        # TODO: We may have multiple pilots.
                        # TODO: Check for errors?
                        logger.debug('Canceling Pilot.')
                        pmgr.cancel_pilots(uids=self.pilot_uid)
                        logger.debug('Pilot canceled.')
                except asyncio.CancelledError as e:
                    logger.debug('Dispatcher context manager received cancellation while exiting.')
                    cancelled_error = e
                except Exception as e:
                    logger.exception('Exception while stopping dispatcher.', exc_info=True)
                    if self._exception:
                        logger.error('Dispatcher is already holding an exception.')
                    else:
                        self._exception = e
                finally:
                    # Should we do any other clean-up here?
                    # self.pilot = None
                    # self._pilot_manager = None
                    if session is not None:
                        if not session.closed:
                            logger.debug('Dispatcher is shutting down RP Session.')
                            try:
                                # Note: close() is not always fast, and we might want to do it asynchronously in the future.
                                session.close()
                            except asyncio.CancelledError as e:
                                cancelled_error = e
                            except Exception as e:
                                logger.exception(f'RADICAL Pilot threw {repr(e)} during Session.close().',
                                                 exc_info=True)
                            else:
                                logger.debug('RP Session closed.')
        if cancelled_error:
            raise cancelled_error

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False

    def __init__(self,
                 source_context: scalems.context.WorkflowManager,
                 loop: asyncio.AbstractEventLoop,
                 command_queue: asyncio.Queue,
                 rp_params: RPParams,
                 dispatcher_lock=None,
                 ):
        """Create a client side execution manager.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """
        if 'RADICAL_PILOT_DBURL' not in os.environ:
            raise DispatchError('RADICAL Pilot environment is not available.')

        if not isinstance(rp_params.target_venv, str) or len(rp_params.target_venv) == 0:
            raise ValueError('Caller must specify a venv to be activated by the execution agent for dispatched tasks.')
        else:
            self._target_venv: str = rp_params.target_venv
            self._pre_exec = ['. ' + str(os.path.join(self._target_venv, 'bin', 'activate'))]

        self.source_context = source_context
        self.command_queue = command_queue

        self.execution_target = rp_params.execution_target
        self._rp_resource_params = {}
        try:
            self._rp_resource_params.update(rp_params.rp_resource_params)
        except TypeError as e:
            raise TypeError(
                f'RP Resource Parameters are expected to be dict-like. Got {repr(rp_params.rp_resource_params)}') from e

        self._loop: asyncio.AbstractEventLoop = loop

        self.session = None
        self._finalizer = None

        if not isinstance(dispatcher_lock, asyncio.Lock):
            raise TypeError('An asyncio.Lock is required to control dispatcher state.')
        self._dispatcher_lock = dispatcher_lock

        self.pilot = None
        self.scheduler = None
        self._exception = None
        self.rp_tasks = []

    def active(self) -> bool:
        session = self.session
        if session is None:
            return False
        else:
            assert session is not None
            return not session.closed

    def shutdown(self):
        if self.active():
            self.session.close()
            assert self.session.closed
            # Is there any reason to reuse a closed Session?
            self.session = None
        else:
            warnings.warn('shutdown has been called more than once.')

    def __del__(self):
        if self.active():
            warnings.warn('{} was not explicitly shutdown.'.format(repr(self)))

    def __await__(self):
        """Implement the asyncio task represented by this object."""
        # Note that this is not a native coroutine object; we cannot `await`
        # The role of object.__await__() is to return an iterator that will be
        # run to exhaustion in the context of an event loop.
        # We assume that most of the asyncio activity happens through the
        # async context mananager behavior and other async member functions.
        # If we choose to `await instance` at all, we need a light-weight
        # iteration we can perform to surrender control of the event loop,
        # and then just do some sort of tidying or reporting that doesn't fit well
        # into __aexit__(), such as the ability to return a value.

        # # Note: We can't do this without first wait on some sort of Done event...
        # failures = []
        # for t in self.rp_tasks:
        #     logger.info('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
        #     if t.state != rp.states.DONE or t.exit_code != 0:
        #         logger.error(f'RP Task unsuccessful: {repr(t)}')
        #         failures.append(t)
        # if len(failures) > 0:
        #     warnings.warn('Unsuccessful tasks: ' + ', '.join([repr(t) for t in failures]))

        yield
        if self._exception:
            raise self._exception
        return self.scheduler

        # # If we want to provide a "Future-like" interface, we should support the callback
        # # protocols and implement the following generator function.
        # if not self.done():
        #     self._asyncio_future_blocking = True
        #     # ref https://docs.python.org/3/library/asyncio-future.html#asyncio.isfuture
        #
        #     yield self  # This tells Task to wait for completion.
        # if not self.done():
        #     raise RuntimeError("The dispatcher task was not 'await'ed.")
        # Ref PEP-0380: "return expr in a generator causes StopIteration(expr)
        # to be raised upon exit from the generator."
        # The Task works like a `result = yield from awaitable` expression.
        # The iterator (generator) yields until exhausted,
        # then raises StopIteration with the value returned in by the generator function.
        # return self.result()  # May raise too.
        # # Otherwise, the only allowed value from the iterator is None.


class ExecutionContext:
    """WorkflowManager for the Executor side of workflow session dispatching through RADICAL Pilot."""

    def __init__(self):
        self.__rp_cfg = dict()
        if 'RADICAL_PILOT_DBURL' not in os.environ:
            raise DispatchError('RADICAL Pilot environment is not available.')

        resource = 'local.localhost'
        # TODO: Find default config?
        resource_config = {resource: {}}
        # TODO: Get from user or local config files.
        resource_config[resource].update({
            'project': None,
            'queue': None,
            'schema': None,
            'cores': 1,
            'gpus': 0
        })
        pilot_description = dict(resource=resource,
                                 runtime=30,
                                 exit_on_error=True,
                                 project=resource_config[resource]['project'],
                                 queue=resource_config[resource]['queue'],
                                 cores=resource_config[resource]['cores'],
                                 gpus=resource_config[resource]['gpus'])
        self.resource_config = resource_config
        self.pilot_description = pilot_description
        self.session = None
        self._finalizer = None
        self.umgr = None
