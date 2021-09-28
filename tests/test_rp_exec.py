"""Dispatch scalems.exec through RADICAL Pilot.

In the first draft, we keep the tests simpler by assuming we are invoked in an
environment where RP is already configured. In a follow-up, we will use a
dispatching layer to run meaningful tests through an RP Context specialization.
In turn, the initial RP dispatching will probably use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.

Note: `export RADICAL_LOG_LVL=DEBUG` to enable RP debugging output.
"""
import asyncio
import logging
import os
import sys
import time

import pytest

import scalems
import scalems.context
import scalems.radical
import scalems.radical.runtime
import scalems.workflow

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.


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
        rp_resource_params={'PilotDescription': {'access_schema': pilot_description.access_schema}}
    )

    # Test RPDispatcher context
    manager = scalems.radical.workflow_manager(loop)

    # This sleep doesn't cost too much waiting, but seems to effectively work around
    # some sort of race condition as resources are freed when running the full test suite.
    time.sleep(10)
    # TODO: Try to find a better way to wait for previous resources to be released.

    with scalems.workflow.scope(manager):
        assert not loop.is_closed()
        # Enter the async context manager for the default dispatcher
        cmd1 = scalems.executable(('/bin/echo',))
        async with manager.dispatch(params=params) as dispatcher:
            assert isinstance(dispatcher, scalems.radical.RPDispatchingExecutor)
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

    import gc
    # gc.set_debug(gc.DEBUG_LEAK)

    # `manager = None` Does not finalize the FileStoreManager because there are
    # references to the WorkflowManager or FileStoreManager in several frames still
    # floating around. Consider only passing WorkflowManager by weakref, etc.
    assert sys.getrefcount(manager) == 4
    # At this point there are 4 references to `manager`, though 1 would be removed
    # through garbage collection.
    # gc.collect()
    # assert sys.getrefcount(manager) == 3
    # TODO: Get ref count for manager down to 1 without garbage collection and confirm
    #  that the following results in finalization.
    del manager
    # logger.debug(gc.garbage)
    gc.collect()  # Forces the FileStoreManager generator to be closed.
    # Rather than worry about when gc happens, we could use an alternate protocol,
    # or just allow a WorkflowManager.close() method.

    # Test active context scoping.
    assert scalems.workflow.get_scope() is original_context
    assert not loop.is_closed()


@pytest.mark.skip(reason='Unimplemented.')
@pytest.mark.asyncio
async def test_batch():
    """Run a batch of uncoupled tasks, dispatched through RP."""
    assert False
