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
import contextlib
import copy
import inspect
import json
import logging
import os
import queue
# Note that we import RP early to allow it to monkey-patch some modules early.
import shlex
import typing
import warnings
import weakref
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Optional
from typing import Tuple

import radical.pilot as rp

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
            rp_resource_params = getattr(self, '_rp_resource_params', None)

            # Note: the executor owns a rp.Session during operation. Manage scope
            # of executor operation with a context manager. RP does not yet use an event loop,
            # but we can use async context manager for future compatibility with asyncio
            # management of network connections, etc.
            # TODO: Provide the handles to the relevant locks.
            executor = RPDispatchingExecutor(source_context=self,
                                             command_queue=executor_queue,
                                             execution_target=execution_target,
                                             rp_resource_params=rp_resource_params,
                                             dispatcher_lock=self._dispatcher_lock)

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
            # TODO: Consider awaiting Session launch and more of the dispatcher initialization before yielding.
            executor_task = asyncio.ensure_future(executor)
            # asyncio.create_task(dispatcher)

        try:
            # We can surrender control here and leave the executor and dispatcher tasks active
            # while evaluating a `with` block suite for the `dispatch` context manager.
            async with executor as dispatching_session:
                yield dispatching_session
            # We don't have a use for the executor_task right now, and it might be removed soon.
            # Note, though, that this would be the appropriate way to receive a final state or
            # result from the executor.
            executor_result = await executor_task
            assert executor_result

        except Exception as e:
            logger.exception('Uncaught exception while in dispatching context: {}'.format(str(e)))
            raise e

        finally:

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

            # Check that the queue drained.
            # WARNING: The queue will never finish draining if executor_task fails.
            #  I.e. don't `await executor_queue.join()`
            if not executor_queue.empty():
                raise InternalError('Bug: Executor left tasks in the queue without raising an exception.')

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


async def rp_task_coroutine(task: rp.Task) -> asyncio.Future:
    """Mediate between a radical.pilot.Task and an asyncio.Future.

    Schedule an asyncio Task to receive the result of the RP Task. The asyncio
    Task must also make sure that asyncio cancellation propagates to the rp.Task.cancel,
    and vice versa.

    This function should be awaited immediately to make sure the necessary call-backs
    get registered. The result will be an asyncio.Future, which should be awaited
    separately.

    Internally, this function provides a call-back to the rp.Task. The call-back
    provided to RP cannot directly call asyncio.Future methods (such as set_result() or set_exception())
    because RP will be making the call from another thread without mediation by the
    asyncio event loop.

    As such, we also need to provide a thread-safe event handler to propagate the
    RP Task call-back to the asyncio Future.
    """
    if not isinstance(task, rp.Task):
        raise TypeError('Function requires a RADICAL Pilot Task object.')
    _rp_task = task

    loop = asyncio.get_running_loop()
    _future = loop.create_future()

    async def finalizer(task: rp.Task):
        """This is the coroutine function that will be scheduled by the rp callback."""
        if task.state == rp.states.DONE:
            _future.set_result(task.as_dict())
        elif task.state in (rp.states.CANCELED, rp.states.CANCELING):
            _future.cancel()
        elif task.state == rp.states.FAILED:
            _future.set_exception(RPTaskFailure(task=task))
        else:
            raise ValueError('Unexpected state: {}'.format(str(task.state)))

    def rp_callback(obj: rp.Task, state):
        """Called when the rp.Task state changes."""
        logger.debug(f'Callback triggered by {repr(obj)} state change to {repr(state)}.')
        assert obj is _rp_task

        if state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED):
            # TODO: unregister call-back with RP Task or redirect subsequent call-backs.

            # Schedule a coroutine to run in the original event loop.
            future = asyncio.run_coroutine_threadsafe(finalizer(obj), loop)

            # Wait for the result with an optional timeout argument
            assert future.result(1)
        else:
            # Note: debugging only!
            # TODO: Get rid of this soon!
            raise ValueError('unknown state: {}'.format(repr(state)))

    async def watch():
        """Provide a coroutine that we can schedule to watch the RP Task."""
        try:
            while not _future.done():
                # Do we not get a callback trigger for cancellations?
                if _rp_task.state in (rp.states.CANCELED,):
                    _future.cancel()
                else:
                    await asyncio.sleep(1)
        except asyncio.CancelledError as e:
            if _rp_task.state not in (rp.states.CANCELED,):
                # TODO: We shoulld unregister the callback first...
                _rp_task.cancel()
            raise e

    task.register_callback(rp_callback)
    asyncio.create_task(watch())
    return _future


class RPDispatchingExecutor:
    """Client side manager for work dispatched through RADICAL Pilot.

    Configuration points::
    * resource config
    * pilot config
    * session config?
    """
    command_queue: asyncio.Queue
    execution_target: str
    pilot: rp.Pilot
    scheduler: typing.Union[rp.Task, None]
    session: typing.Union[rp.Session, None]
    source_context: scalems.context.WorkflowManager
    rp_tasks: typing.Iterable[rp.Task]

    _dispatcher_lock: asyncio.Lock
    _pilot_description: rp.PilotDescription
    _pilot_manager: rp.PilotManager
    _rp_resource_params: typing.Optional[dict]
    _task_description: rp.TaskDescription
    _task_manager: rp.TaskManager
    _queue_runner_task: asyncio.Task

    async def __aenter__(self):
        # Get a lock while the state is changing.
        async with self._dispatcher_lock:
            self._connect_rp()

            # Launch queue processor (proxy executor).
            self._queue_runner_task = self.start()
            assert isinstance(self._queue_runner_task, asyncio.Task)

        # Note: it would probably be most useful to return something with a WorkflowManager
        # interface...
        return self

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

        """
        # Note that we cannot resolve the full _resource config until we have a Session object.

        #
        # Start the Session.
        #

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
        logger.debug('Entering RP dispatching context.')

        # This would be a good time to `await`, if an event-loop friendly Session creation function becomes available.
        self.session = rp.Session(uid=session_id, cfg=session_config)
        session_id = self.session.uid
        session_config = copy.deepcopy(self.session.cfg.as_dict())

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
            {'resource': 'local.localhost',
             'cores': 4,
             'gpus': 0})

        # We can launch an initial Pilot, but we may have to run further Pilots
        # during self._queue_runner_task (or while servicing scalems.wait() within the with block)
        # to handle dynamic work load requirements.
        # Optionally, we could refrain from launching the pilot here, at all,
        # but it seems like a good chance to start bootstrapping the agent environment.
        self._pilot_manager = rp.PilotManager(session=self.session)

        # How and when should we update pilot description?
        self.pilot = self._pilot_manager.submit_pilots(self._pilot_description)

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

        rp_spec = 'radical.pilot@git+https://github.com/radical-cybertools/radical.pilot.git@project/scalems'
        rp_spec = shlex.quote(rp_spec)
        scalems_spec = shlex.quote('scalems@git+https://github.com/SCALE-MS/scale-ms.git@sms-54')
        self.pilot.prepare_env(
            {
                'scalems_env': {
                    'type': 'virtualenv',
                    'version': '3.8',
                    'setup': [
                        # TODO: Generalize scalems dependency resolution.
                        # Ideally, we would check the current API version requirement, map that to a package version,
                        # and specify >=min_version, allowing cached archives to satisfy the dependency.
                        # 'file:///tmp/pycharm_project_147',
                        rp_spec,
                        scalems_spec
                    ]}})
        # # Could we stage in archive distributions directly?
        # # self.pilot.stage_in()

        self._task_manager = rp.TaskManager(session=self.session)
        # Question: when should we remove the pilot from the task manager?
        self._task_manager.add_pilots(self.pilot)

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
        rp_check = self._task_manager.submit_tasks(
            rp.TaskDescription(
                {
                    # 'executable': py_venv,
                    'executable': 'python3',
                    'arguments': ['-c', 'import radical.pilot as rp; print(rp.version)'],
                    'named_env': 'scalems_env'
                }
            )
        )
        sms_check = self._task_manager.submit_tasks(
            rp.TaskDescription(
                {
                    # 'executable': py_venv,
                    'executable': 'python3',
                    'arguments': [
                        '-c',
                        "import pkg_resources; print(pkg_resources.resource_filename('scalems.radical',"
                        + " 'data/scalems_test_cfg.json'))"
                    ],
                    'named_env': 'scalems_env'
                }
            )
        )
        if self._task_manager.wait_tasks(uids=[rp_check.uid])[0] != rp.states.DONE or rp_check.exit_code != 0:
            raise DispatchError('Could not verify RP in execution environment.')
        if rp_check.stdout != '1.6.0\n':
            raise DispatchError('Could not get a valid execution environment.')

        scalems_is_installed = self._task_manager.wait_tasks(uids=[sms_check.uid])[
                                   0] == rp.states.DONE and sms_check.exit_code == 0
        if not scalems_is_installed:
            raise DispatchError('Could not verify scalems in execution environment.')

        # # Verify usable SCALEMS RP connector.
        # task = self._task_manager.submit_tasks(
        #     rp.TaskDescription(
        #         {
        #             'executable': 'python3',
        #             'arguments': ['-c',
        #             "import pkg_resources;" +
        #             " print(pkg_resources.resource_filename('scalems.radical', 'data/scalems_test_cfg.json'))"],
        #             'named_env': 'scalems_env'
        #         }
        #     )
        # )
        # # Check whether scalems is installed in the target execution environment.
        # scalems_is_installed = self._task_manager.wait_tasks(uids=[task.uid])[
        #                            0] == rp.states.DONE and task.exit_code == 0

        # TODO: Check for compatible installed scalems API version.

        #
        # Get a scheduler task.
        #
        config_file = sms_check.stdout.rstrip()
        # master_script = pkg_resources.get_entry_info('scalems', 'console_scripts', 'scalems_rp_agent').name
        master_script = 'scalems_rp_agent'
        # worker_script = pkg_resources.get_entry_info('scalems', 'console_scripts', 'scalems_rp_worker').name
        # worker_script = 'scalems_rp_worker'

        # assert pkg_resources.resource_exists('scalems.radical', 'data/scalems_rp_agent.py')
        # master_script = Path(pkg_resources.resource_filename('scalems.radical', 'data/scalems_rp_agent.py'))
        # assert master_script.exists()
        #
        # assert pkg_resources.resource_exists('scalems.radical', 'data/scalems_rp_worker.py')
        # worker_script = Path(pkg_resources.resource_filename('scalems.radical', 'data/scalems_rp_worker.py'))
        # assert worker_script.exists()
        #
        # assert pkg_resources.resource_exists('scalems.radical', 'data/scalems_test_cfg.json')
        # config_file = Path(pkg_resources.resource_filename('scalems.radical', 'data/scalems_test_cfg.json'))
        # assert config_file.exists()
        # assert isinstance(config_file.name, str)

        # # TODO: When we can assert that the scalems package is installed in the agent venv,
        # #    we don't need to stage the files anymore.
        # self._task_description = rp.TaskDescription(
        #     {
        #         'uid'          :  'raptor.scalems',
        #         'executable'   :  'python3',
        #         'arguments'    : ['./scalems_rp_master.py', 'scalems_test_cfg.json'],
        #         'input_staging': [config_file.name,
        #                           master_script.name,
        #                           worker_script.name]
        #     })
        self._task_description = rp.TaskDescription(
            {
                'uid': 'raptor.scalems',
                'executable': master_script,
                'arguments': [config_file],
                'input_staging': [],
                # 'pre_exec': ['. {}'.format(os.path.join(venv, 'bin', 'activate'))],
                'named_env': 'scalems_env'
            })

        # Launch main (scheduler) RP task.
        try:
            task = self._task_manager.submit_tasks(self._task_description)
            if isinstance(task, rp.Task):
                self.scheduler = task
            else:
                raise InternalError(f'Expected a Task. Got {repr(task)}.')
        except Exception as e:
            raise DispatchError('Failed to launch SCALE-MS master task.') from e

    def start(self) -> asyncio.Task:
        """Start the queue processor.

        Launch an asyncio task to process the queue.

        The launched task is finalized with the corresponding *stop()* method.
        """
        if self.session is None or self.session.closed:
            raise ProtocolError('Cannot process queue without a RP Session.')

        # Define the raptor.scalems tasks and submit them to the master (scheduler)
        def test_workload():
            tds = list()
            for i in range(2):
                uid = 'scalems.%06d' % i
                # ------------------------------------------------------------------
                # work serialization goes here
                # This dictionary is interpreted by rp.raptor.Master.
                work = json.dumps({'mode': 'call',
                                   'cores': 1,
                                   'timeout': 10,
                                   'data': {'method': 'hello',
                                            'kwargs': {'world': uid}}})
                # ------------------------------------------------------------------
                tds.append(rp.TaskDescription({
                    'uid': uid,
                    'executable': 'scalems',
                    # This field is ignored by the ScaleMSMaster that receives this submission.
                    'scheduler': 'raptor.scalems',  # 'scheduler' references the task implemented as a
                    'arguments': [work],  # Processed by raptor.Master._receive_tasks
                    # 'output_staging': []
                    'named_env': 'scalems_env'
                }))
            tasks = self._task_manager.submit_tasks(tds)
            assert len(tasks) == len(tds)
            return tasks

        # Client side queue management goes here.
        self.rp_tasks = test_workload()

        runner_task = asyncio.create_task(self._queue_runner())
        return runner_task

    async def _queue_runner(self):
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
            finally:
                self.command_queue.task_done()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up at context exit.

        In addition to handling exceptions, the main resource to clean up is the rp.Session.

        We also need to make sure that we properly disengage from any queues or generators.

        We can also leave flags for ourself to be checked at __await__, if there is a Task
        associated with the Executor.
        """
        async with self._dispatcher_lock:
            session: rp.Session = self.session
            # This method is not thread safe, but we try to make clear that instance.session
            # is no longer publicly available as soon as possible.
            self.session = None

            if session is None or session.closed:
                logger.error('rp.Session is already closed?!')
            else:
                await self.stop()

            # Should we do any other clean-up here?
            # self.pilot = None
            # self._pilot_manager = None

            if session is not None:
                if not session.closed:
                    try:
                        # Note: close() is not always fast, and we might want to do it asynchronously in the future.
                        session.close()
                    except Exception as e:
                        logger.exception(f'RADICAL Pilot threw {repr(e)} during Session.close().')

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False

    async def stop(self):
        """Shut down the queuing and processing of commands.

        Stop accepting new items and drain the queue.
        """
        # We are able to make start() a non-async method. We may be able to do the
        # same for stop() if there were a task that we could loop.run_until_complete().
        # Doing so might force us to make sure that the concurrency regions in our code
        # are clean, and it may simplify debugging, but I'm not sure it is worthwhile.

        # Stop the executor.
        logger.debug('Stopping the SCALEMS RP dispatching executor.')
        # TODO: Make sure that nothing else will be adding items to the queue from this point.
        # We should establish some assurance that the next line represents the last thing
        # that we will put in the queue.
        self.command_queue.put_nowait({'control': 'stop'})

        # TODO: Check that we were actually started and running!
        # If the queue is not being processed, join() will never return.
        await self.command_queue.join()
        logger.debug('Command queue is empty.')

        # done, pending = await asyncio.wait(aws, timeout=0.1, return_when=FIRST_EXCEPTION)
        # Currently, the queue runner does not place subtasks, so there is only one thing to await.
        await self._queue_runner_task
        logger.debug('Queue runner task completed.')
        if self._queue_runner_task.exception() is not None:
            raise self._queue_runner_task.exception()

        states = self._task_manager.wait_tasks(uids=[t.uid for t in self.rp_tasks])
        assert states
        for t in self.rp_tasks:
            logger.info('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
            if t.state != rp.states.DONE or t.exit_code != 0:
                logger.error(f'RP Task unsuccessful: {repr(t)}')

        # Cancel the master.
        self._task_manager.cancel_tasks(uids=self.scheduler.uid)
        # Cancel blocks until the task is done so the following wait would (currently) be redundant,
        # but there is a ticket open to change this behavior.
        # See https://github.com/radical-cybertools/radical.pilot/issues/2336
        # tmgr.wait_tasks([scheduler.uid])

    def __init__(self,
                 source_context: scalems.context.WorkflowManager,
                 command_queue=asyncio.Queue,
                 execution_target: str = 'local.localhost',
                 rp_resource_params: dict = None,
                 dispatcher_lock=None
                 ):
        """Create a client side execution manager.

        Initialization and deinitialization occurs through
        the Python (async) context manager protocol.
        """
        if 'RADICAL_PILOT_DBURL' not in os.environ:
            raise DispatchError('RADICAL Pilot environment is not available.')

        self.source_context = source_context
        self.command_queue = command_queue

        self.execution_target = execution_target
        self._rp_resource_params = rp_resource_params

        self.session = None
        self._finalizer = None

        if not isinstance(dispatcher_lock, asyncio.Lock):
            raise TypeError('An asyncio.Lock is required to control dispatcher state.')
        self._dispatcher_lock = dispatcher_lock

        self.pilot = None
        self.scheduler = None

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
