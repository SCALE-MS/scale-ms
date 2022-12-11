"""Dispatch scalems.exec through RADICAL Pilot.

For testing purposes, the RP dispatching can use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.
Refer to the repository directories ``docker`` and ``.github/workflows``.

Note: ``export RADICAL_LOG_LVL=DEBUG`` to enable RP debugging output.
"""

import asyncio
import os

import packaging.version
import pytest

import scalems
import scalems.context
import scalems.messages
import scalems.radical
import scalems.radical.runtime
import scalems.workflow

try:
    import radical.pilot as rp
except ImportError:
    rp = None
else:
    import scalems.radical.raptor

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.

pytestmark = pytest.mark.skipif(condition=rp is None,
                                reason='These tests require RADICAL Pilot.')

client_scalems_version = packaging.version.Version(scalems.__version__)
if client_scalems_version.is_prerelease:
    minimum_scalems_version = client_scalems_version.public
else:
    minimum_scalems_version = client_scalems_version.base_version


# We either need to mock a RP agent or gather coverage files from remote environments.
# This test does not add any coverage, and only adds measurable coverage or coverage
# granularity if we either mock a RP agent or gather coverage files from remote environments.
@pytest.mark.asyncio
async def test_raptor_master(pilot_description, rp_venv, cleandir):
    """Check our ability to launch and interact with a Master task."""
    import radical.pilot as rp

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip('This test requires a user-provided static RP venv.')

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    # Configure module.
    params = scalems.radical.runtime.Configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={
            'PilotDescription': pilot_description.as_dict()
        }
    )

    to_thread = scalems.utility.get_to_thread()

    manager = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(manager, close_on_exit=True):
        async with manager.dispatch(params=params) as dispatcher:
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f'test_raptor_master Session is {repr(dispatcher.runtime.session)}')

            # Bypass the scalems machinery and submit an instruction directly to the master task.
            scheduler: rp.Task = dispatcher.runtime.scheduler
            td = rp.TaskDescription()
            td.scheduler = scheduler.uid
            td.mode = scalems.radical.raptor.CPI_MESSAGE
            td.metadata = scalems.messages.HelloCommand().encode()
            logger.debug(f'Submitting {str(td.as_dict())}')
            tasks = await to_thread(dispatcher.runtime.task_manager().submit_tasks, [td])
            hello_task = tasks[0]
            logger.debug(f'Submitted {str(hello_task.as_dict())}. Waiting...')
            state = await to_thread(hello_task.wait, state=rp.FINAL, timeout=180)
            logger.debug(str(hello_task.as_dict()))

            td.metadata = scalems.messages.StopCommand().encode()
            logger.debug(f'Submitting {str(td.as_dict())}')
            tasks = await to_thread(dispatcher.runtime.task_manager().submit_tasks, [td])
            stop_task = tasks[0]

            # Note: Once `stop` is issued, the client will never see the Task complete. I.e. the following would fail:
            # stop_watcher = asyncio.create_task(
            #     to_thread(stop_task.wait, state=rp.FINAL, timeout=180), name='stop-watcher')
            # await asyncio.wait_for(stop_watcher, timeout=120)
            assert stop_task.state not in rp.FINAL

            # Note: We don't actually have anything to keep us from canceling the Master task
            # before the work has been handled.

    assert state == rp.DONE
    assert hello_task.stdout == repr(scalems.radical.raptor.backend_version)
    # Ref https://github.com/SCALE-MS/scale-ms/discussions/268
    # assert task.return_value == scalems.__version__


# def test_rp_raptor_remote_docker(sdist, rp_task_manager):
#     """Test the core RADICAL Pilot functionality that we rely on through ssh-based execution."""
#     import radical.pilot as rp
#     tmgr = rp_task_manager
#
#     # TODO: How can we recover successful workflow stages from previous failed Sessions?
#     #
#     # The client needs to note the sandbox locations from runs. SCALEMS can
#     # then manage / maintain state tracking or state discovery to optimize workflow recovery.
#     # Resumed workflows can make reference to sandboxes from previous sessions
#     # (RCT work in progress: https://github.com/radical-cybertools/radical.pilot/tree/feature/sandboxes
#     # slated for merge in 2021 Q2 to support `sandbox://` URIs).
#
#     # define a raptor.scalems master and launch it within the pilot
#     pwd   = os.path.dirname(__file__)
#     td    = rp.TaskDescription(
#             {
#                 'uid'          :  'raptor.scalems',
#                 'executable'   :  'python3',
#                 'arguments'    : ['./scalems_test_master.py', '%s/scalems_test_cfg.json'  % pwd],
#                 'input_staging': ['%s/scalems_test_cfg.json'  % pwd,
#                                   '%s/scalems_test_master.py' % pwd,
#                                   '%s/scalems_test_worker.py' % pwd]
#             })
#     scheduler = tmgr.submit_tasks(td)
#
#     # define raptor.scalems tasks and submit them to the master
#     tds = list()
#     for i in range(2):
#         uid  = 'scalems.%06d' % i
#         # ------------------------------------------------------------------
#         # work serialization goes here
#         # This dictionary is interpreted by rp.raptor.Master.
#         work = json.dumps({'mode'      :  'call',
#                            'cores'     :  1,
#                            'timeout'   :  10,
#                            'data'      : {'method': 'hello',
#                                           'kwargs': {'world': uid}}})
#         # ------------------------------------------------------------------
#         tds.append(rp.TaskDescription({
#                            'uid'       : uid,
# The *executable* field is ignored by the ScaleMSMaster that receives this submission.
#                            'executable': 'scalems',
#                            'scheduler' : 'raptor.scalems', # 'scheduler' references the task implemented as a
#                            'arguments' : [work]  # Processed by raptor.Master._receive_tasks
#         }))
#
#     tasks = tmgr.submit_tasks(tds)
#     assert len(tasks) == len(tds)
#     # 'arguments' gets wrapped in a Request at the Master by _receive, then
#     # picked up by the Worker in _request_cb. Then picked up in forked interpreter
#     # by Worker._dispatch, which checks the *mode* of the Request and dispatches
#     # according to native or registered mode implementations. (e.g. 'call' (native) or 'scalems')
#
#     # task process is launched with Python multiprocessing (native) module and added to self._pool.
#     # When the task runs, it's result triggers _result_cb
#
#     # wait for *those* tasks to complete and report results
#     tmgr.wait_tasks(uids=[t.uid for t in tasks])
#
#     # Cancel the master.
#     tmgr.cancel_tasks(uids=scheduler.uid)
#     # Cancel blocks until the task is done so the following wait it currently redundant,
#     # but there is a ticket open to change this behavior.
#     # See https://github.com/radical-cybertools/radical.pilot/issues/2336
#     # tmgr.wait_tasks([scheduler.uid])
#
#     for t in tasks:
#         print('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
#         assert t.state == rp.states.DONE
#         assert t.exit_code == 0


@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@pytest.mark.asyncio
async def test_exec_rp(pilot_description, rp_venv, cleandir):
    """Test that we are able to launch and shut down a RP dispatched execution session.
    """
    import radical.pilot as rp

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip('This test requires a user-provided static RP venv.')

    original_context = scalems.workflow.get_scope()
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    # Configure module.
    params = scalems.radical.runtime.Configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={
            'PilotDescription': pilot_description.as_dict()
        }
    )

    # Test RPDispatcher context
    manager = scalems.radical.workflow_manager(loop)

    # This sleep doesn't cost too much waiting, but seems to effectively work around
    # some sort of race condition as resources are freed when running the full test suite.
    await asyncio.sleep(60)
    # TODO: Try to find a better way to wait for previous resources to be released.

    with scalems.workflow.scope(manager, close_on_exit=True):
        assert not loop.is_closed()
        # Enter the async context manager for the default dispatcher
        cmd1 = scalems.executable(('/bin/echo',))
        async with manager.dispatch(params=params) as dispatcher:
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f'exec_rp Session is {repr(dispatcher.runtime.session)}')
            cmd2 = scalems.executable(('/bin/echo', 'hello', 'world'))
            # TODO: Clarify whether/how result() method should work in this scope.
            # TODO: Make scalems.wait(cmd) work as expected in this scope.
        assert cmd1.done()
        assert cmd2.done()
        logger.debug(cmd1.result())
        logger.debug(cmd2.result())

    # TODO: Output typing.
    out1: rp.Task = cmd1.result()
    for output in out1.description['output_staging']:
        assert os.path.exists(output['target'])
    out2: rp.Task = cmd2.result()
    for output in out2.description['output_staging']:
        assert os.path.exists(output['target'])
        if output['target'].endswith('stdout'):
            with open(output['target'], 'r') as fh:
                line = fh.readline()
                assert line.rstrip() == 'hello world'

    # Test active context scoping.
    assert scalems.workflow.get_scope() is original_context
    assert not loop.is_closed()


@pytest.mark.skip(reason='Unimplemented.')
@pytest.mark.asyncio
async def test_batch():
    """Run a batch of uncoupled tasks, dispatched through RP."""
    assert False
