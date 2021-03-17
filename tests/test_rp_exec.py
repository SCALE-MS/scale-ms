#!/usr/bin/env python3

"""Dispatch scalems.exec through RADICAL Pilot.

In the first draft, we keep the tests simpler by assuming we are invoked in an
environment where RP is already configured. In a follow-up, we will use a
dispatching layer to run meaningful tests through an RP Context specialization.
In turn, the initial RP dispatching will probably use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.
"""
import asyncio
import json
import logging
import os
import subprocess
import warnings

import pytest
import typing

import scalems
import scalems.context
import scalems.radical

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.


def test_rp_static_venv(rp_venv, pilot_description):
    """Confirm that a prepared venv is usable by RP.

    .. todo:: Are we using this for the Pilot? Or just the Tasks?
    """
    # If scalems is installed and usable, and the venv is activated,
    # then the `scalems_rp_agent` entry point script should be discoverable with `which`
    # and executable.
    ...


def test_rp_usability(pilot_description):
    """Confirm availability of RADICAL Pilot infrastructure.

    Tests here may be too cumbersome to run in every invocation of a
    pytest fixture, so let's just run them once in this unit test.
    """

    import radical.pilot as rp

    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=DeprecationWarning)
        with rp.Session() as session:
            resource = session.get_resource_config(pilot_description.resource)
            assert resource


def test_rp_basic_task_local(rp_task_manager, pilot_description):
    if pilot_description.resource != 'local.localhost' \
            and pilot_description.access_schema \
            and pilot_description.access_schema != 'local':
        pytest.skip('This test is only for local execution.')

    from radical.pilot import TaskDescription
    tmgr = rp_task_manager

    td = TaskDescription({'executable': '/bin/date',
                             'cpu_processes': 1})
    task = tmgr.submit_tasks(td)
    tmgr.wait_tasks(uids=[task.uid])

    assert task.exit_code == 0


def test_rp_basic_task_remote(rp_task_manager, pilot_description):
    import radical.pilot as rp

    if pilot_description.resource == 'local.localhost':
        pytest.skip('This test is only for remote execution.')

    tmgr = rp_task_manager

    td = rp.TaskDescription({'executable': '/usr/bin/hostname',
                             'cpu_processes': 1})

    task = tmgr.submit_tasks(td)

    tmgr.wait_tasks(uids=[task.uid])

    assert task.state == rp.states.DONE
    assert task.exit_code == 0

    localname = subprocess.run(['/usr/bin/hostname'], stdout=subprocess.PIPE, encoding='utf-8').stdout.rstrip()
    remotename = task.stdout.rstrip()
    assert len(remotename) > 0
    assert remotename != localname


def test_prepare_venv(rp_task_manager, sdist):
    # NOTE: *sdist* is a path of an sdist archive that we could stage for the venv installation.
    # QUESTION: Can't we use the radical.pilot package archive that was already placed for bootstrapping the pilot?

    # TODO: Merge with test_rp_raptor_local but use the installed scalems_rp_agent and scalems_rp_worker files
    # Do we want to copy the config or generate it? It can be retrieved programmatically with
    #     pkg_resources.resource_filename('scalems.radical', 'data/scalems_test_cfg.json')

    import radical.pilot as rp
    # We only expect one pilot
    pilot: rp.Pilot = rp_task_manager.get_pilots()[0]
    # We get a dictionary...
    # assert isinstance(pilot, rp.Pilot)
    # But it looks like it has the pilot id in it.
    pilot_uid = typing.cast(dict, pilot)['uid']
    pmgr_uid = typing.cast(dict, pilot)['pmgr']
    session: rp.Session = rp_task_manager.session
    pmgr: rp.PilotManager = session.get_pilot_managers(pmgr_uids=pmgr_uid)
    assert isinstance(pmgr, rp.PilotManager)
    pilot = pmgr.get_pilots(uids=pilot_uid)
    assert isinstance(pilot, rp.Pilot)
    # It looks like either the pytest fixture should deliver something other than the TaskManager,
    # or the prepare_venv part should be moved to a separate function, such as in conftest...

    tmgr = rp_task_manager

    pilot.prepare_env({
        'scalems_env': {
            'type': 'virtualenv',
            'version': '3.8',
            'setup': [
                'radical.pilot@git+https://github.com/radical-cybertools/radical.pilot.git@project/scalems',
                'scalems@git+https://github.com/SCALE-MS/scale-ms.git@sms-54'
            ]}})

    td = rp.TaskDescription({'executable': 'python3',
                             'arguments': ['-c',
                                           'import radical.pilot as rp;'
                                           'import scalems;'
                                           'print(rp.version_detail);'
                                           'print(scalems.__file__)'],
                             'named_env': 'scalems_env'})
    task = tmgr.submit_tasks(td)
    tmgr.wait_tasks()
    logger.info(task.stdout)
    assert task.exit_code == 0


@pytest.mark.asyncio
async def test_rp_future_cancel_from_rp(rp_task_manager):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp

    tmgr = rp_task_manager

    # Test propagation of RP cancellation behavior
    td = rp.TaskDescription({'executable': '/bin/bash',
                             'arguments': ['-c', '/bin/sleep 5 && /bin/echo success'],
                             'cpu_processes': 1})
    task: rp.Task = tmgr.submit_tasks(td)

    wrapper: asyncio.Future = await scalems.radical.rp_task(task)

    task.cancel()
    try:
        await asyncio.wait_for(wrapper, timeout=120)
    except asyncio.CancelledError:
        ...
    except asyncio.TimeoutError as e:
        # Temporary point to insert an easy debugging break point
        raise e

    # Note: The cancellation occurs through the `watch` coroutine, rather than through the rp callback.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2348
    assert wrapper.cancelled()
    assert task.state == rp.states.CANCELED


@pytest.mark.asyncio
async def test_rp_future_propagate_cancel(rp_task_manager):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp
    tmgr = rp_task_manager

    # Test propagation of asyncio cancellation behavior.
    td = rp.TaskDescription({'executable': '/bin/bash',
                             'arguments': ['-c', '/bin/sleep 5 && /bin/echo success'],
                             'cpu_processes': 1})
    task: rp.Task = tmgr.submit_tasks(td)

    wrapper: asyncio.Task = await scalems.radical.rp_task(task)

    assert isinstance(wrapper, asyncio.Task)
    wrapper.cancel()
    try:
        await asyncio.wait_for(wrapper, timeout=5)
    except asyncio.CancelledError:
        ...
    except asyncio.TimeoutError as e:
        # Temporary point to insert an easy debugging break point
        raise e
    assert wrapper.cancelled()

    # WARNING: rp.Task.wait() never completes with no arguments.
    task.wait(state=rp.states.CANCELED, timeout=120)
    assert task.state in (rp.states.CANCELED,)
    # This test is initially showing that the callback is triggered
    # for several state changes as the task runs to completion without being canceled.


@pytest.mark.asyncio
async def test_rp_future(rp_task_manager):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp
    tmgr = rp_task_manager

    # Test run to completion
    td = rp.TaskDescription({'executable': '/bin/bash',
                             'arguments': ['-c', '/bin/sleep 5 && /bin/echo success'],
                             'cpu_processes': 1})
    task: rp.Task = tmgr.submit_tasks(td)

    wrapper: asyncio.Task = await scalems.radical.rp_task(task)

    try:
        result = await asyncio.wait_for(wrapper, timeout=120)
    except asyncio.TimeoutError as e:
        # Temporary point to insert an easy debugging break point
        # raise e
        result = None
    assert task.exit_code == 0
    assert 'success' in task.stdout

    assert 'stdout' in result
    assert 'success' in result['stdout']


@pytest.mark.xfail
def test_rp_scalems_environment_preparation_local(rp_task_manager):
    """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.

    This test function specifically tests the local.localhost resource.

    Note that we cannot wait on the environment preparation directly, but we can define
    a task with ``named_env`` matching the *prepare_env* key to implicitly depend on
    successful creation.
    """
    assert False


@pytest.mark.xfail
def test_staging(sdist, rp_task_manager):
    """Confirm that we are able to bundle and install the package currently being tested."""
    # Use the `sdist` fixture to bundle the current package.
    # Copy the sdist archive to the RP target resource.
    # Create through an RP task that includes the sdist as input staging data.
    # Unpack and install the sdist.
    # Confirm matching versions.
    assert False


@pytest.mark.xfail
def test_rp_scalems_environment_preparation_remote_docker(rp_task_manager):
    """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.

    This test function specifically tests ssh-based dispatching to a resource
    other than local.localhost. We will use a mongodb and sshd running in Docker.
    """
    assert False


def test_rp_raptor_local(rp_task_manager):
    """Test the core RADICAL Pilot functionality that we rely on using local.localhost.

    Use sample 'master' and 'worker' scripts for to exercise the RP "raptor" features.
    """
    import radical.pilot as rp
    tmgr = rp_task_manager

    # TODO: Use master and worker scripts from the installed scalems package.

    # define a raptor.scalems master and launch it within the pilot
    pwd = os.path.dirname(__file__)
    td = rp.TaskDescription(
        {
            'uid': 'raptor.scalems',
            'executable': 'python3',
            'arguments': ['./scalems_test_master.py', '%s/scalems_test_cfg.json' % pwd],
            'input_staging': ['%s/scalems_test_cfg.json' % pwd,
                              '%s/scalems_test_master.py' % pwd,
                              '%s/scalems_test_worker.py' % pwd]
        })
    scheduler = tmgr.submit_tasks(td)

    # define raptor.scalems tasks and submit them to the master
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
            'executable': 'scalems',  # This field is ignored by the ScaleMSMaster that receives this submission.
            'scheduler': 'raptor.scalems',  # 'scheduler' references the task implemented as a
            'arguments': [work]  # Processed by raptor.Master._receive_tasks
        }))

    tasks = tmgr.submit_tasks(tds)
    assert len(tasks) == len(tds)
    # 'arguments' gets wrapped in a Request at the Master by _receive, then
    # picked up by the Worker in _request_cb. Then picked up in forked interpreter
    # by Worker._dispatch, which checks the *mode* of the Request and dispatches
    # according to native or registered mode implementations. (e.g. 'call' (native) or 'scalems')

    # task process is launched with Python multiprocessing (native) module and added to self._pool.
    # When the task runs, it's result triggers _result_cb

    # wait for *those* tasks to complete and report results
    tmgr.wait_tasks(uids=[t.uid for t in tasks])

    # Cancel the master.
    tmgr.cancel_tasks(uids=scheduler.uid)
    # Cancel blocks until the task is done so the following wait it currently redundant,
    # but there is a ticket open to change this behavior.
    # See https://github.com/radical-cybertools/radical.pilot/issues/2336
    # tmgr.wait_tasks([scheduler.uid])

    for t in tasks:
        print('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
        assert t.state == rp.states.DONE
        assert t.exit_code == 0


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


# TODO: Provide PilotDescription to dispatcher.
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@pytest.mark.asyncio
async def test_exec_rp(pilot_description):
    """Test that we are able to launch and shut down a RP dispatched execution session.

    TODO: Where should we specify the target resource? An argument to *dispatch()*?
    """
    original_context = scalems.context.get_context()
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    # Test RPDispatcher context
    context = scalems.radical.RPWorkflowContext(loop)
    with scalems.context.scope(context):
        assert not loop.is_closed()
        # Enter the async context manager for the default dispatcher
        async with context.dispatch():
            ...

        # TODO: re-enable test for basic executable wrapper.
        # async with context.dispatch():
        #     cmd = scalems.executable(('/bin/echo',))

    # Test active context scoping.
    assert scalems.context.get_context() is original_context
    assert not loop.is_closed()


@pytest.mark.xfail(strict=True)
@pytest.mark.asyncio
async def test_batch():
    """Run a batch of uncoupled tasks, dispatched through RP."""
    assert False


@pytest.mark.xfail(strict=True)
@pytest.mark.asyncio
async def test_chained_commands():
    """Run a sequence of two tasks with a data flow dependency."""
    assert False


@pytest.mark.xfail(strict=True)
@pytest.mark.asyncio
async def test_file_staging():
    """Test a simple SCALE-MS style command chain that places a file and then retrieves it."""
    assert False
