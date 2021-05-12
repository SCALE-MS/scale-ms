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
# TODO: Consider converting to a namespace package to improve modularity of
#  implementation.

import argparse
import asyncio
import contextlib
import dataclasses
import functools
import json
import logging
import os
import pathlib
import threading
import typing

from radical import pilot as rp

import scalems.execution
import scalems.subprocess
import scalems.workflow
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.exceptions import ScaleMSError
from scalems.execution import AbstractWorkflowUpdater
from scalems.execution import RuntimeManager
from scalems.workflow import ResourceType
from .runtime import _configuration
from .runtime import _connect_rp
from .runtime import _set_configuration
from .runtime import Configuration
from .runtime import get_pre_exec
from .runtime import parser as _runtime_parser
from .runtime import Runtime

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)


@cache
def parser(add_help=False):
    """Get the module-specific argument parser.

    Provides a base argument parser for scripts using the scalems.radical backend.

    By default, the returned ArgumentParser is created with ``add_help=False``
    to avoid conflicts when used as a *parent* for a parser more local to the caller.
    If *add_help* is provided, it is passed along to the ArgumentParser created
    in this function.

    See Also:
         https://docs.python.org/3/library/argparse.html#parents
    """
    _parser = argparse.ArgumentParser(add_help=add_help, parents=[_runtime_parser()])
    # We don't yet have anything to add...
    return _parser


def configuration(*args, **kwargs) -> Configuration:
    """Get (and optionally set) the RADICAL Pilot configuration.

    With no arguments, returns the current configuration. If a configuration has
    not yet been set, the command line parser is invoked to try to build a new
    configuration.

    If arguments are provided, try to construct a scalems.radical.Configuration
    and use it to initialize the module.

    It is an error to try to initialize the module more than once.
    """
    # Not thread-safe
    if len(args) > 0:
        _set_configuration(*args, **kwargs)
    elif len(kwargs) > 0:
        _set_configuration(
            Configuration(**kwargs)
        )
    elif _configuration.get(None) is None:
        # No config is set yet. Generate with module parser.
        c = Configuration()
        parser().parse_known_args(namespace=typing.cast(argparse.Namespace, c))
        _configuration.set(c)
    return _configuration.get()


def executor_factory(manager: scalems.workflow.WorkflowManager,
                     params: Configuration = None):
    if params is not None:
        _set_configuration(params)
    params = configuration()

    executor = RPDispatchingExecutor(source=manager,
                                     loop=manager.loop(),
                                     configuration=params,
                                     dispatcher_lock=manager._dispatcher_lock)
    return executor


def workflow_manager(loop: asyncio.AbstractEventLoop):
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
    return scalems.workflow.WorkflowManager(loop=loop, executor_factory=executor_factory)


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

    To use, partially bind the *final* parameter (with functools.partial) to get a
    callable with the RP.Task callback signature.

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


async def _rp_task_watcher(task: rp.Task,  # noqa: C901
                           future: asyncio.Future, final: RPFinalTaskState,
                           ready: asyncio.Event) -> rp.Task:
    """Manage the relationship between an RP.Task and a scalems Future.

    Cancel the RP.Task if this task or the scalems.Future is canceled.

    Publish the RP.Task result or cancel the scalems.Future if the RP.Task is
    done or canceled.

    Arguments:
        task: RADICAL Pilot Task, submitted by caller.
        future: asyncio.Future to which *task* results should be propagated.
        ready: output parameter, set when coroutine has run enough to perform its
        responsibilities.

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

    Caller should await the *ready* event before assuming the watcher task is doing its
    job.
    """
    try:
        ready.set()

        def finished():
            return task.state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED) \
                   or future.done() \
                   or final

        while not finished():
            # Let the watcher wake up periodically to check for state changes.
            # TODO: (#96) Use a control thread to manage *threading* primitives and
            #  translate to asyncio primitives.
            done, pending = await asyncio.wait([future],
                                               timeout=0.05,
                                               return_when=asyncio.FIRST_COMPLETED)
            if future.cancelled():
                assert future in done
                if task.state != rp.states.CANCELED:
                    logger.debug(
                        'Propagating cancellation from scalems future to rp task.')
                    task.cancel()
                return task
            if final:
                logger.debug(f'Handling finalization for RP task {task.uid}')
                if final.failed.is_set():
                    if not future.cancelled():
                        assert not future.done()
                        assert future in pending
                        logger.debug('Propagating RP Task failure.')
                        # TODO: Provide more useful error feedback.
                        future.set_exception(RPTaskFailure(f'{task.uid} failed.',
                                                           task=task))
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
                    logger.debug(f'RP Task {task.uid} complete, but Event not '
                                 'triggered. Possible race condition.')
    except asyncio.CancelledError as e:
        logger.debug(
            'Propagating scalems manager task cancellation to scalems future and rp '
            'task.')
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
    provided to RP cannot directly call asyncio.Future methods (such as set_result() or
    set_exception()) because RP will be making the call from another thread without
    mediation by the asyncio event loop.

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

    final = RPFinalTaskState()
    callback = functools.partial(_rp_callback, final=final)
    functools.update_wrapper(callback, _rp_callback)
    rptask.register_callback(callback)

    asyncio.get_running_loop().slow_callback_duration = 0.2
    watcher_started = asyncio.Event()
    waiter = asyncio.create_task(watcher_started.wait())
    wrapped_task = asyncio.create_task(_rp_task_watcher(task=rptask,
                                                        future=future,
                                                        final=final,
                                                        ready=watcher_started))

    # Make sure that the task is cancellable before returning it to the caller.
    await asyncio.wait((waiter, wrapped_task),
                       return_when=asyncio.FIRST_COMPLETED)
    if wrapped_task.done():
        # Let CancelledError propagate.
        e = wrapped_task.exception()
        if e is not None:
            raise e
    # watcher_task.
    return wrapped_task


def _describe_legacy_task(item: scalems.workflow.Task,
                          pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    For a "raptor" style task, see _describe_raptor_task()
    """
    subprocess_type = ResourceType(('scalems', 'subprocess', 'SubprocessTask'))
    assert item.description().type() == subprocess_type
    input_data = item.input
    task_input = scalems.subprocess.SubprocessInput(**input_data)
    args = list([arg for arg in task_input.argv])
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    task_description = rp.TaskDescription(from_dict=dict(executable=args[0],
                                                         arguments=args[1:],
                                                         stdout=str(task_input.stdout),
                                                         stderr=str(task_input.stderr),
                                                         pre_exec=pre_exec))
    uid: str = item.uid().hex()
    task_description.uid = uid

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = list(task_input.inputs.values())
    task_description.output_staging = [{
        'source': str(task_input.stdout),
        'target': os.path.join(uid, pathlib.Path(task_input.stdout).name),
        'action': rp.TRANSFER
    }, {
        'source': str(task_input.stderr),
        'target': os.path.join(uid, pathlib.Path(task_input.stderr).name),
        'action': rp.TRANSFER
    }]
    task_description.output_staging += task_input.outputs.values()

    return task_description


def _describe_raptor_task(item: scalems.workflow.Task,
                          scheduler: str,
                          pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    The TaskDescription will be submitted to the named *scheduler*,
    where *scheduler* is the UID of a task managing the life of a rp.raptor.Master
    instance.

    Caller is responsible for ensuring that *scheduler* is valid.
    """
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    # Ref: scalems_rp_master._RaptorTaskDescription
    task_description = rp.TaskDescription(
        from_dict=dict(
            executable='scalems',  # This value is currently ignored, but must be set.
            pre_exec=pre_exec
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


async def submit(*,
                 item: scalems.workflow.Task,
                 task_manager: rp.TaskManager,
                 pre_exec: list,
                 scheduler: str = None) -> asyncio.Task:
    """Dispatch a WorkflowItem to be handled by RADICAL Pilot.

    Registers a Future for the task result with *item*.

    Args:
        item: The workflow item to be submitted
        task_manager: A radical.pilot.TaskManager instance
                      through which the task should be submitted.
        scheduler (str): The string name of the "scheduler," corresponding to
                         the UID of a Task running a rp.raptor.Master.

    Returns an asyncio.Task for a submitted rp.Task.

    The caller *must* await the result of the coroutine to obtain an asyncio.Task that
    can be cancelled or awaited as a proxy to direct RP task management. The Task will
    hold a coroutine that is guaranteed to already be running, failed, or canceled. The
    caller should check the status of the task immediately before making assumptions
    about whether a Future has been successfully bound to the managed workflow item.

    The *submitted* (output) event is likely a short-term placeholder and subject to
    change. For instance, the use case for waiting on such an event could be met by
    waiting on the state change of the workflow item to a SUBMITTED state. However,
    note that this function will block for a short time at the
    rp.TaskManager.submit_tasks() call, so it is useful to separate the submission
    event from the completion of this coroutine early in development while we decide
    whether and how to relegate RP calls to threads separated from that of the event
    loop.

    The returned asyncio.Task can be used to cancel the rp.Task (and the Future)
    or to await the RP.Task cleanup.

    To submit tasks as a batch, await an array of submit_rp_task() results in the
    same dispatching context. (TBD)

    Notes:

        workflow manager maintains the workflow state without expensive or stateful
        volatile resources, and can mediate updates to the managed workflow at any
        time. Items enter the graph in an IDLE state. The WorkflowManager can provide
        Futures for the results of the managed items. For IDLE items,
        the WorkflowManager retains a weakref to the issued Futures, which it can use
        to make sure that there is only zero or one Future for a particular result.

        WorkflowManager collaborates with Queuer to transition the graph to an "active"
        or "executing" state. This transition is mediated through the dispatcher_lock.

        Queuer sequences and queues workflow items to be handled, pushing them to a
        dispatch_queue. No state change to the workflow item seems necessary at this
        time.

        The dispatch_queue is read by an ExecutionManager. Items may be processed
        immediately or staged in a command_queue. Workflow items are then either
        SUBMITTED or BLOCKED (awaiting dependencies). Optionally, Items may be marked
        ELIGIBLE and re-queued for batch submission.

        If the ExecutionManager is able to submit a task, the Task has a call-back
        registered for the workflow item. The WorkflowManager needs to convert any
        Future weakrefs to strong references when items are SUBMITTED, and the workflow
        Futures are subscribed to the item. Tasks are wrapped in a scalems object that
        the WorkflowManager is able to take ownership of. BLOCKED items are wrapped in
        Tasks which are subscribed to their dependencies (WorkflowItems should already
        be subscribed to WorkflowItem Futures for any dependencies) and stored by the
        ExecutionManager. When the call-backs for all of the dependencies indicate the
        Item should be processed into an upcoming workload, the Item becomes ELIGIBLE,
        and its wrapper Task (in collaboration with the ExecutionManager) puts it in
        the command_queue.

        As an optimization, and to support co-scheduling, a WorkflowItem call-back can
        provide notification of state changes. For instance, a BLOCKED item may become
        ELIGIBLE once all of its dependencies are SUBMITTED, when the actual Executor
        has some degree of data flow management capabilities.

    """

    # TODO: Optimization: skip tasks that are already done (cached results available).
    def scheduler_is_ready(scheduler):
        return isinstance(scheduler, str) \
               and len(scheduler) > 0 \
               and isinstance(task_manager.get_tasks(scheduler), rp.Task)

    subprocess_type = ResourceType(('scalems', 'subprocess', 'SubprocessTask'))
    if item.description().type() == subprocess_type:
        if scheduler is not None:
            raise DispatchError('Raptor not yet supported for scalems.executable.')
        rp_task_description = _describe_legacy_task(item, pre_exec=pre_exec)
    elif scheduler_is_ready(scheduler):
        # We might want a contextvars.Context to hold the current rp.Master instance name.
        rp_task_description = _describe_raptor_task(item, scheduler, pre_exec=pre_exec)
    else:
        raise APIError('Caller must provide the UID of a submitted *scheduler* task.')

    loop = asyncio.get_running_loop()
    rp_task_result_future = loop.create_future()

    # Warning: in the long run, we should not extend the life of the reference returned
    # by edit_item, and we need to consider the robust way to publish item results.
    # TODO: Translate RP result to item result type.
    rp_task_result_future.add_done_callback(functools.partial(scalems_callback,
                                                              item=item))

    # TODO: Move slow blocking RP calls to a separate RP control thread.
    task = task_manager.submit_tasks(rp_task_description)

    rp_task_watcher = await rp_task(rptask=task, future=rp_task_result_future)

    if rp_task_watcher.done():
        if rp_task_watcher.cancelled():
            raise DispatchError(f'Task for {item} was unexpectedly canceled during '
                                'dispatching.')
        e = rp_task_watcher.exception()
        if e is not None:
            raise DispatchError('Task for {item} failed during dispatching.') from e

    return rp_task_watcher


def scalems_callback(fut: asyncio.Future, *, item: scalems.workflow.Task):
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


def _get_scheduler(name: str,
                   pre_exec: typing.Iterable[str],
                   task_manager: rp.TaskManager):
    """Establish the radical.pilot.raptor.Master task.

    Create a master rp.Task (running the scalems_rp_master script) with the
    provide *name* to be referenced as the *scheduler* for raptor tasks.

    Returns the rp.Task for the master script once the Master is ready to
    receive submissions.

    Raises:
        DispatchError if the master task could not be launched successfully.

    Note:
        Currently there is no completion condition for the master script.
        Caller is responsible for canceling the Task returned by this function.
    """
    # This is the name that should be resolvable in an active venv for the script we
    # install as
    # pkg_resources.get_entry_info('scalems', 'console_scripts', 'scalems_rp_master').name
    master_script = 'scalems_rp_master'

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
            'uid': name,
            'executable': master_script
        })
    td.arguments = []
    td.pre_exec = pre_exec
    # td.named_env = 'scalems_env'
    logger.debug('Launching RP scheduler.')
    scheduler = task_manager.submit_tasks(td)
    # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not
    # check type.
    scheduler.wait(state=[rp.states.AGENT_EXECUTING] + rp.FINAL)
    if scheduler.state not in {rp.states.CANCELED, rp.states.FAILED}:
        raise DispatchError('Could not get Master task for dispatching.')
    return scheduler


class RPDispatchingExecutor(RuntimeManager):
    """Client side manager for work dispatched through RADICAL Pilot.

    Configuration points::
    * resource config
    * pilot config
    * session config?
    """

    def __init__(self,
                 source: scalems.workflow.WorkflowManager,
                 *,
                 loop: asyncio.AbstractEventLoop,
                 configuration: Configuration,
                 dispatcher_lock=None):
        """Create a client side execution manager.

        Initialization and de-initialization occurs through
        the Python (async) context manager protocol.
        """
        if 'RADICAL_PILOT_DBURL' not in os.environ:
            raise DispatchError('RADICAL Pilot environment is not available.')

        if not isinstance(configuration.target_venv, str) \
                or len(configuration.target_venv) == 0:
            raise ValueError(
                'Caller must specify a venv to be activated by the execution agent for '
                'dispatched tasks.')
        super().__init__(source,
                         loop=loop,
                         configuration=configuration,
                         dispatcher_lock=dispatcher_lock)

    @contextlib.contextmanager
    def runtime_configuration(self):
        """Provide scoped Configuration.

        Merge the runtime manager's configuration with the global configuration,
        update the global configuration, and yield the configuration for a ``with`` block.

        Restores the previous global configuration when exiting the ``with`` block.

        Warning:
            We do not check for re-entrance, which will cause race conditions w.r.t.
            which Context state is restored! Moreover, the Configuration object is not
            currently hashable and does not have an equality test defined.

        TODO:
            Reconsider this logic.

        Design notes:
            Do we want two-way interaction between module
            and instance configuration? Under what circumstances will one or the other
            change during execution? Should we be providing the configuration through
            the current Context, through a Context instance (usable for Context.run() or
            to the task launching command), or simply as a Configuration object?
        """

        # Get default configuration.
        configuration_dict = dataclasses.asdict(configuration())
        # Update with any internal configuration.
        if self._runtime_configuration.target_venv is not None and len(
                self._runtime_configuration.target_venv) > 0:
            configuration_dict['target_venv'] = self._runtime_configuration.target_venv
        if len(self._runtime_configuration.rp_resource_params) > 0:
            configuration_dict['rp_resource_params'].update(
                self._runtime_configuration.rp_resource_params)
        if self._runtime_configuration.execution_target is not None \
                and len(self._runtime_configuration.execution_target) > 0:
            configuration_dict[
                'execution_target'] = self._runtime_configuration.execution_target
        c = Configuration(**configuration_dict)
        token = _configuration.set(c)
        try:
            yield c
        finally:
            _configuration.reset(token)

    def runtime_startup(self, runner_started: asyncio.Event) -> asyncio.Task:
        configuration: Configuration = self.configuration()
        self.runtime = _connect_rp(configuration)

        if self.runtime is None or self.runtime.session.closed:
            raise ProtocolError('Cannot process queue without a RP Session.')

        # Launch queue processor (proxy executor).
        # TODO: Make runtime_startup optional. Let it return a resource that is
        #  provided to the normalized run_executor(), or maybe use it to configure the
        #  Submitter that will be provided to the run_executor.
        runner_task = asyncio.create_task(
            scalems.execution.manage_execution(
                self,
                processing_state=runner_started))
        # TODO: Note the expected scope of the runner_task lifetime with respect to
        #  the global state changes (i.e. ContextVars and locks).
        return runner_task

    def runtime_shutdown(self, runtime: Runtime):
        session = getattr(runtime, 'session', None)
        if session is None or session.closed:
            logger.error('Runtime Session is already closed?!')
        else:
            # Cancel the master.
            logger.debug('Canceling the master scheduling task.')
            task_manager = runtime.task_manager()
            if runtime.scheduler is not None:
                task_manager.cancel_tasks(uids=runtime.scheduler.uid)
                # Cancel blocks until the task is done so the following wait is
                # (currently) redundant, but there is a ticket open to change this
                # behavior.
                # See https://github.com/radical-cybertools/radical.pilot/issues/2336
                runtime.scheduler.wait(state=rp.FINAL)
                logger.debug('Master scheduling task complete.')

            # TODO: We may have multiple pilots.
            # TODO: Check for errors?
            logger.debug('Canceling Pilot.')
            runtime.pilot().cancel()
            logger.debug('Pilot canceled.')
            runtime.task_manager().close()
            logger.debug('TaskManager closed.')
            runtime.pilot_manager().close()
            logger.debug('PilotManager closed.')
            session.close()
            if session.closed:
                logger.debug('Session closed.')
            else:
                logger.error('Session not closed!')
        logger.debug('Runtime shut down.')

    def updater(self) -> 'WorkflowUpdater':
        return WorkflowUpdater(executor=self)


class WorkflowUpdater(AbstractWorkflowUpdater):
    def __init__(self, executor: RPDispatchingExecutor):
        self.executor = executor
        self.task_manager = executor.runtime.task_manager()
        # TODO: Make sure we are clear about the scope of the configuration and the
        #  life time of the workflow updater / submitter.
        self._pre_exec = list(get_pre_exec(executor.configuration()))

    async def submit(self, *, item: scalems.workflow.Task) -> asyncio.Task:
        # TODO: Ensemble handling
        item_shape = item.description().shape()
        if len(item_shape) != 1 or item_shape[0] != 1:
            raise MissingImplementationError(
                'Executor cannot handle multidimensional tasks yet.')

        task: asyncio.Task[rp.Task] = await submit(item=item,
                                                   task_manager=self.task_manager,
                                                   pre_exec=self._pre_exec)
        return task


class ExecutionContext:
    """WorkflowManager for running tasks when dispatching through RADICAL Pilot."""

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
