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
# Note that we import RP early to allow it to monkey-patch some modules early.
import contextvars
import dataclasses
import functools
import json
import logging
import os
import threading
import typing
import warnings

import packaging.version
from radical import pilot as rp

from .. import context as _context
from ..context import QueueItem
from ..exceptions import APIError
from ..exceptions import DispatchError
from ..exceptions import InternalError
from ..exceptions import MissingImplementationError
from ..exceptions import ProtocolError
from ..exceptions import ScaleMSError

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# TODO: Consider scoping for these Workflow context variables.
# Need to review PEP-567 and PEP-568 to consider where and how to scope the Context
# with respect to the dispatching scope.
execution_target = contextvars.ContextVar('execution_target', default='local.localhost')
resource_params = contextvars.ContextVar('resource_params')
# TODO: Consider alternatives for getting a default venv.
target_venv = contextvars.ContextVar('target_venv', default='/home/rp/rp-venv')


@dataclasses.dataclass
class RPParams:
    execution_target: str = 'local.localhost'
    rp_resource_params: dict = dataclasses.field(default_factory=dict)
    target_venv: str = None


def executor_factory(context: _context.WorkflowManager):
    try:
        rp_resource_params = resource_params.get()
    except LookupError:
        rp_resource_params = {}

    executor = RPDispatchingExecutor(source_context=context,
                                     loop=context.loop(),
                                     rp_params=RPParams(
                                         execution_target=execution_target.get(),
                                         rp_resource_params=rp_resource_params,
                                         target_venv=target_venv.get()
                                     ),
                                     dispatcher_lock=context._dispatcher_lock,
                                     )
    return executor


class RPWorkflowContext(_context.WorkflowManager):
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
        self._executor_factory = executor_factory
        super(RPWorkflowContext, self).__init__(loop)


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


async def _rp_task_watcher(task: rp.Task, future: asyncio.Future, final: RPFinalTaskState,
                           ready: asyncio.Event) -> rp.Task:
    """Manage the relationship between an RP.Task and a scalems Future.

    Cancel the RP.Task if this task or the scalems.Future is canceled.

    Publish the RP.Task result or cancel the scalems.Future if the RP.Task is
    done or canceled.

    Arguments:
        task: RADICAL Pilot Task, submitted by caller.
        future: asyncio.Future to which *task* results should be propagated.
        ready: output parameter, set when coroutine has run enough to perform its responsibilities.

    Returns:
        *task* in its final state.

    An asyncio.Future based on this coroutine has very similar semantics to the
    required *future* argument, but this is subject to change.
    The coroutine is intended to facilitate progress of the task,
    regardless of the rp.Task results. The provided *future* allows the rp.Task
    results to be interpreted and semantically translated. rp.Task failure is
    translated into an exception on *future*. The *future* has a different
    exposure than the coroutine return value, as well: again, the *future* is
    connected to the workflow item and user-facing interface, whereas this
    coroutine is a detail of the task management. Still, these two modes of output
    are subject to revision without notice.

    Caller should await the *ready* event before assuming the watcher task is doing its job.
    """
    try:
        ready.set()
        finished = lambda: task.state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED) \
                           or future.done() \
                           or final
        while not finished():
            # Let the watcher wake up periodically to check for state changes.
            # TODO: (#96) Use a control thread to manage *threading* primitives and translate to asyncio primitives.
            done, pending = await asyncio.wait([future], timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
            if future.cancelled():
                if task.state != rp.states.CANCELED:
                    logger.debug('Propagating cancellation from scalems future to rp task.')
                    task.cancel()
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
                        result = task.as_dict()
                        future.set_result(result)
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

    asyncio.get_running_loop().slow_callback_duration = 0.2
    watcher_started = asyncio.Event()
    wrapped_task = asyncio.create_task(_rp_task_watcher(task=rptask, future=future, final=final, ready=watcher_started))
    # Make sure that the task is cancellable before returning it to the caller.
    await watcher_started.wait()
    # watcher_task.
    return wrapped_task


def _describe_task(item: _context.Task, scheduler: str) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    The TaskDescription will be submitted to the named *scheduler*,
    where *scheduler* is the UID of a task managing the life of a rp.raptor.Master instance.

    Caller is responsible for ensuring that *scheduler* is valid.
    """
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    # Ref: scalems_rp_master._RaptorTaskDescription
    task_description = rp.TaskDescription(
        from_dict=dict(
            executable='scalems'  # This value is currently ignored, but must be set.
        )
    )
    task_description.uid = item.uid()
    task_description.scheduler = str(scheduler)
    # Example work would be the JSON serialized form of the following dictionary.
    # {'mode': 'call',
    #  'cores': 1,
    #  'timeout': 10,
    #  'data': {'method': 'hello',
    #           'kwargs': {'world': uid}}}
    #
    # Maybe something like this:
    # work_dict = {
    #     'mode': 'scalems',
    #     'cores': 1,
    #     'timeout': 10,
    #     'data': item.serialize()
    # }
    work_dict = {
        'mode': 'exec',
        'cores': 1,
        'timeout': None,
        'data': {
            'exe': item.input['argv'][0],
            'args': item.input['argv'][1:]
        }
    }
    task_description.arguments = [json.dumps(work_dict)]

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = []
    task_description.output_staging = []

    return task_description


async def submit(item: _context.Task, task_manager: rp.TaskManager, submitted: asyncio.Event = None,
                 scheduler: str = None) -> asyncio.Task:
    """Dispatch a WorkflowItem to be handled by RADICAL Pilot.

    Registers a Future for the task result with *item*.

    Args:
        item: The workflow item to be submitted
        task_manager: A radical.pilot.TaskManager instance through which the task should be submitted.
        submitted: (Output parameter) Caller-provided Event to be set once the RP Task has been submitted.
        scheduler (str): The string name of the "scheduler," corresponding to the UID of a Task running a rp.raptor.Master.

    Returns an asyncio.Task for a submitted rp.Task.

    The caller should immediately await *submitted* (or the result of this coroutine)
    to ensure that the task has actually been submitted. The caller *must* await
    the result of the coroutine to obtain an asyncio.Task that can be cancelled or
    awaited as a proxy to direct RP task management.

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

        WorkflowManager collaborates with Queuer to transition the graph to an "active" or "executing" state.
        This transition is mediated through the dispatcher_lock.

        Queuer sequences and queues workflow items to be handled, pushing them to a dispatch_queue. No state
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
    rp_task_result_future.add_done_callback(functools.partial(scalems_callback, item=item))

    # TODO: Move slow blocking RP calls to a separate RP control thread.
    task = task_manager.submit_tasks(rp_task_description)
    # TODO: What checks can/should we do to allow checks that dependencies have been successfully scheduled?
    if isinstance(submitted, asyncio.Event):
        submitted.set()

    rp_task_watcher = await rp_task(rptask=task, future=rp_task_result_future)

    return rp_task_watcher


def scalems_callback(fut: asyncio.Future, *, item: _context.Task):
    """Process the completed Future for an rp.Task.

    Partially bind *item* to use this as the argument to *fut.add_done_callback()*.

    Warning: in the long run, we should not extend the life of the reference returned
    by edit_item, and we need to consider the robust way to publish item results.
    """
    assert fut.done()
    if fut.cancelled():
        logger.info(f'Task supporting {item} has been cancelled.')
    else:
        if fut.exception():
            logger.info(f'Task supporting {item} failed: {fut.exception()}')
        else:
            # TODO: Construct an appropriate scalems Result from the rp Task.
            item.set_result(fut.result())


class RPDispatchingExecutor:
    """Client side manager for work dispatched through RADICAL Pilot.

    Configuration points::
    * resource config
    * pilot config
    * session config?
    """
    execution_target: str
    scheduler: typing.Union[rp.Task, None]
    session: typing.Union[rp.Session, None]
    source_context: _context.WorkflowManager
    submitted_tasks: typing.List[asyncio.Task]

    _command_queue: asyncio.Queue
    _dispatcher_lock: asyncio.Lock
    _pilot_description: rp.PilotDescription
    _rp_resource_params: typing.Optional[dict]
    _task_description: rp.TaskDescription
    _queue_runner_task: asyncio.Task

    def __init__(self,
                 source_context: _context.WorkflowManager,
                 loop: asyncio.AbstractEventLoop,
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

        # TODO: Only hold a queue in an active context manager.
        self._command_queue = asyncio.Queue()

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
        self.submitted_tasks = []

    def queue(self):
        # TODO: Only expose queue while in an active context manager.
        return self._command_queue

    async def __aenter__(self):
        try:
            # Get a lock while the state is changing.
            # The dispatching protocol is immature. Initially, we don't expect contention for the lock, and if there is
            # contention, it probably represents an unintended race condition or systematic dead-lock.
            # TODO: Clarify dispatcher state machine and remove/replace assertions.
            assert not self._dispatcher_lock.locked()
            async with self._dispatcher_lock:
                self._connect_rp()
                if self.session is None or self.session.closed:
                    raise ProtocolError('Cannot process queue without a RP Session.')

                # Launch queue processor (proxy executor).
                runner_started = asyncio.Event()
                task_manager = self.session.get_task_managers(tmgr_uids=self._task_manager_uid)
                runner_task = asyncio.create_task(run_executor(self,
                                                               task_manager=task_manager,
                                                               processing_state=runner_started,
                                                               queue=self._command_queue))
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
            # Do we want to log this somewhere?
            # session_config = copy.deepcopy(self.session.cfg.as_dict())
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

            # This is the name that should be resolvable in an active venv for the script we install
            # as pkg_resources.get_entry_info('scalems', 'console_scripts', 'scalems_rp_master').name
            master_script = 'scalems_rp_master'

            scheduler_name = 'raptor.scalems'
            # We can probably make the config file a permanent part of the local metadata,
            # but we don't really have a scheme for managing local metadata right now.
            # with tempfile.TemporaryDirectory() as dir:
            #     config_file_name = 'raptor_scheduler_config.json'
            #     config_file_path = os.path.join(dir, config_file_name)
            #     with open(config_file_path, 'w') as fh:
            #         encoded = scalems_rp_master.encode_as_dict(scheduler_config)
            #         json.dump(encoded, fh, indent=2)

            # define a raptor.scalems master and launch it within the pilot
            td = rp.TaskDescription(
                {
                    'uid': scheduler_name,
                    'executable': master_script})
            td.arguments = []
            td.pre_exec = self._pre_exec
            # td.named_env = 'scalems_env'
            logger.debug('Launching RP scheduler.')
            scheduler = task_manager.submit_tasks(td)
            # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not check type.
            scheduler.wait(state=[rp.states.AGENT_EXECUTING] + rp.FINAL)

            assert self.scheduler is None
            self.scheduler = scheduler
            # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
            # Note: The worker script name only appears in the config file.
            logger.info('RP scheduler ready.')
            logger.debug(repr(self.scheduler))

        except asyncio.CancelledError as e:
            raise e
        except Exception as e:
            raise DispatchError('Failed to launch SCALE-MS master task.') from e

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up at context exit.

        In addition to handling exceptions, the main resource to clean up is the rp.Session.

        We also need to make sure that we properly disengage from any queues or generators.

        We can also leave flags for ourself to be checked at __await__, if there is a Task
        associated with the Executor.
        """
        # Note that this coroutine could take a long time and could be cancelled at several points.
        cancelled_error = None
        # The dispatching protocol is immature. Initially, we don't expect contention for the lock, and if there is
        # contention, it probably represents an unintended race condition or systematic dead-lock.
        # TODO: Clarify dispatcher state machine and remove/replace assertions.
        assert not self._dispatcher_lock.locked()
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
                    self._command_queue.put_nowait({'control': 'stop'})
                    # TODO: Consider what to do differently when we want to cancel work rather than just finalize it.

                    # done, pending = await asyncio.wait(aws, timeout=0.1, return_when=FIRST_EXCEPTION)
                    # Currently, the queue runner does not place subtasks, so there is only one thing to await.
                    # TODO: We should probably allow the user to provide some sort of timeout, or infer one from other time limits.
                    try:
                        await self._queue_runner_task
                    except asyncio.CancelledError as e:
                        raise e
                    except Exception as queue_runner_exception:
                        logger.exception('Unhandled exception when stopping queue handler.', exc_info=True)
                        self._exception = queue_runner_exception
                    else:
                        logger.debug('Queue runner task completed.')
                    finally:
                        if not self._command_queue.empty():
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
                        if len(self.submitted_tasks) > 0:
                            results = await asyncio.gather(*self.submitted_tasks)

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
                    logger.debug(f'{self.__class__.__qualname__} context manager received cancellation while exiting.')
                    cancelled_error = e
                except Exception as e:
                    logger.exception('Exception while stopping dispatcher.', exc_info=True)
                    if self._exception:
                        logger.error('Queuer is already holding an exception.')
                    else:
                        self._exception = e
                finally:
                    # Should we do any other clean-up here?
                    # self.pilot = None
                    # self._pilot_manager = None
                    if session is not None:
                        if not session.closed:
                            logger.debug('Queuer is shutting down RP Session.')
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

    def exception(self) -> typing.Union[None, Exception]:
        return self._exception

    # We don't currently have a use for a stand-alone Task.
    # We use the async context manager and the exception() method.
    # def __await__(self):
    #     """Implement the asyncio task represented by this object."""
    #     # Note that this is not a native coroutine object; we cannot `await`
    #     # The role of object.__await__() is to return an iterator that will be
    #     # run to exhaustion in the context of an event loop.
    #     # We assume that most of the asyncio activity happens through the
    #     # async context mananager behavior and other async member functions.
    #     # If we choose to `await instance` at all, we need a light-weight
    #     # iteration we can perform to surrender control of the event loop,
    #     # and then just do some sort of tidying or reporting that doesn't fit well
    #     # into __aexit__(), such as the ability to return a value.
    #
    #     # # Note: We can't do this without first wait on some sort of Done event...
    #     # failures = []
    #     # for t in self.rp_tasks:
    #     #     logger.info('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
    #     #     if t.state != rp.states.DONE or t.exit_code != 0:
    #     #         logger.error(f'RP Task unsuccessful: {repr(t)}')
    #     #         failures.append(t)
    #     # if len(failures) > 0:
    #     #     warnings.warn('Unsuccessful tasks: ' + ', '.join([repr(t) for t in failures]))
    #
    #     yield
    #     if self._exception:
    #         raise self._exception
    #     return self.scheduler
    #
    #     # # If we want to provide a "Future-like" interface, we should support the callback
    #     # # protocols and implement the following generator function.
    #     # if not self.done():
    #     #     self._asyncio_future_blocking = True
    #     #     # ref https://docs.python.org/3/library/asyncio-future.html#asyncio.isfuture
    #     #
    #     #     yield self  # This tells Task to wait for completion.
    #     # if not self.done():
    #     #     raise RuntimeError("The dispatcher task was not 'await'ed.")
    #     # Ref PEP-0380: "return expr in a generator causes StopIteration(expr)
    #     # to be raised upon exit from the generator."
    #     # The Task works like a `result = yield from awaitable` expression.
    #     # The iterator (generator) yields until exhausted,
    #     # then raises StopIteration with the value returned in by the generator function.
    #     # return self.result()  # May raise too.
    #     # # Otherwise, the only allowed value from the iterator is None.


async def run_executor(executor: RPDispatchingExecutor, *, processing_state: asyncio.Event, queue: asyncio.Queue,
                       task_manager: rp.TaskManager):
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
        command: QueueItem = await queue.get()
        try:
            logger.debug(f'Processing command {repr(command)}')
            # TODO: Use formal RPC protocol.
            if 'control' in command:
                if command['control'] == 'stop':
                    logger.debug('Dispatching executor received stop command.')
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
            with executor.source_context.edit_item(key) as item:
                if not isinstance(item, _context.Task):
                    raise InternalError(
                        'Bug: Expected {}.edit_item() to return a _context.Task'.format(repr(executor.source_context)))

                # TODO: Ensemble handling
                item_shape = item.description().shape()
                if len(item_shape) != 1 or item_shape[0] != 1:
                    raise MissingImplementationError('Executor cannot handle multidimensional tasks yet.')

                # Bind the WorkflowManager item to an RP Task.

                # TODO: Optimization: skip tasks that are already done.
                # TODO: Check task dependencies.
                submitted = asyncio.Event()  # Not yet used.
                task: asyncio.Task[rp.Task] = await submit(
                    item,
                    task_manager=task_manager,
                    submitted=submitted,
                    scheduler=executor.scheduler.uid)

                executor.submitted_tasks.append(task)

                # This is the role of the Executor, not the Dispatcher.
                # task_type: _context.ResourceType = item.description().type()
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
        except Exception as e:
            logger.debug('Leaving queue runner due to exception.')
            raise e
        finally:
            # Warning: there is a tiny chance that we could receive a asyncio.CancelledError at this line
            # and fail to decrement the queue.
            logger.debug('Releasing "{}" from command queue.'.format(str(command)))
            queue.task_done()


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
