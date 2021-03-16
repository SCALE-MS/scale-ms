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

import scalems
import scalems.context
import scalems.radical

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.


def get_rp_decorator():
    '''
    Decorator for tests that should be run in a RADICAL Pilot environment only.
    '''

    try:
        import radical.pilot as rp
        import radical.utils as ru

    except ImportError:
        rp = None
        ru = None

    with_radical_only = pytest.mark.skipif(
        rp is None or
        ru is None or
        not os.environ.get('RADICAL_PILOT_DBURL'),
        reason="Test requires RADICAL environment.")

    # NOTE: session creation is a non-trivial operation in RP and
    #       should not be part of a decorator or test, IMHO. if the above is
    #       insufficient, then the test should rightly fail. (AM)

    return with_radical_only


# Decorator for tests that should be run in a RADICAL Pilot environment only.
with_radical_only = get_rp_decorator()


def get_resource_decorator(host, port=None):
    """Get a pytest.skipif decorator for the tests depending on specific resources.

    Determine whether our Docker-based test resource is available.
    Return an appropriately constructed pytest marker.

    If the tests are running within the Docker Compose environment, the *compute*
    service host should be accessible. Assume that the caller has already
    done ``eval $(ssh-agent -s); ssh-add borrowed_key`` where ``borrowed_key``
    has been copied from ``~rp/.ssh/id_rsa.pub`` in the ``scalems/radicalpilot`` image.

    .. todo:: Manage ssh-agent within Python? Paramiko?

    """
    # Try: ``ssh rp@compute /bin/echo success |grep success``, or something like it.
    # We also need to define an appropriate resource. Having done that, what is the
    # minimal test case for RP ssh-based execution?
    args = ['ssh', '-l', 'rp']
    if port is not None:
        args.extend(['-p', str(port)])
    args.extend([host, '/bin/echo', 'success'])
    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        timeout=5,
        encoding='utf-8')
    marker = pytest.mark.skipif(
        result.returncode != 0 or
        result.stdout.rstrip() != 'success',
        reason='Could not ssh to target computing resource.'
    )
    return marker


with_docker_only = get_resource_decorator('compute')
with_tunnel_only = get_resource_decorator('127.0.0.1', '22222')


@with_radical_only
def test_rp_usability():
    """Confirm availability of RADICAL Pilot infrastructure.

    Tests here may be too cumbersome to run in every invocation of a
    pytest fixture, so let's just run them once in this unit test.
    """

    import radical.pilot as rp

    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=DeprecationWarning)
        with rp.Session():
            ...


@with_radical_only
def test_rp_basic_task_local(rpsession):
    import radical.pilot as rp

    # Based on `radical.pilot/examples/config.json`
    # TODO: Does the Session have a default spec for 'local.localhost'?
    #       Can/should we reference it?
    #       https://github.com/radical-cybertools/radical.pilot/issues/2181
    # NOTE: a session does not have a spec, really - the resource config
    #       should be a *static* description of the target resource and
    #       should not need any changing. (AM)
    pd = rp.PilotDescription({'resource': 'local.localhost',
                              'cores': 32,
                              'gpus': 1})

    td = rp.TaskDescription({'executable': '/bin/date',
                             'cpu_processes': 1})

    pmgr = rp.PilotManager(session=rpsession)
    tmgr = rp.TaskManager(session=rpsession)

    pilot = pmgr.submit_pilots(pd)
    task = tmgr.submit_tasks(td)

    tmgr.add_pilots(pilot)
    tmgr.wait_tasks(uids=[task.uid])

    assert task.exit_code == 0


@with_docker_only
def test_rp_basic_task_docker_remote(rpsession):
    import radical.pilot as rp

    # Based on `radical.pilot/examples/config.json`
    # TODO: Does the Session have a default spec for 'local.localhost'?
    #       Can/should we reference it?
    #       https://github.com/radical-cybertools/radical.pilot/issues/2181
    # NOTE: a session does not have a spec, really - the resource config
    #       should be a *static* description of the target resource and
    #       should not need any changing. (AM)
    pd = rp.PilotDescription({'resource': 'local.docker',
                              'cores': 4,
                              'gpus': 0,
                              'access_schema': 'ssh'})

    td = rp.TaskDescription({'executable': '/usr/bin/hostname',
                             'cpu_processes': 1})

    pmgr = rp.PilotManager(session=rpsession)
    tmgr = rp.TaskManager(session=rpsession)

    pilot = pmgr.submit_pilots(pd)
    task = tmgr.submit_tasks(td)

    tmgr.add_pilots(pilot)
    tmgr.wait_tasks(uids=[task.uid])

    assert task.state == rp.states.DONE
    assert task.exit_code == 0

    localname = subprocess.run(['/usr/bin/hostname'], stdout=subprocess.PIPE, encoding='utf-8').stdout.rstrip()
    remotename = task.stdout.rstrip()
    assert len(remotename) > 0
    assert remotename != localname


@with_tunnel_only
def test_rp_basic_task_tunnel_remote(rpsession):
    import radical.pilot as rp

    pd = rp.PilotDescription({'resource': 'local.tunnel',
                              'cores': 4,
                              'gpus': 0,
                              'schema': 'ssh'})

    td = rp.TaskDescription({'executable': '/usr/bin/hostname',
                             'cpu_processes': 1})

    pmgr = rp.PilotManager(session=rpsession)
    tmgr = rp.TaskManager(session=rpsession)

    pilot = pmgr.submit_pilots(pd)
    task = tmgr.submit_tasks(td)

    tmgr.add_pilots(pilot)
    tmgr.wait_tasks(uids=[task.uid])

    assert task.exit_code == 0

    localname = subprocess.run(['/usr/bin/hostname'], stdout=subprocess.PIPE, encoding='utf-8').stdout.rstrip()
    remotename = task.stdout.rstrip()
    assert len(remotename) > 0
    assert remotename != localname


@with_radical_only
@pytest.mark.asyncio
async def test_rp_future_cancel_from_rp(rpsession):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp

    pd = rp.PilotDescription({'resource': 'local.localhost',
                              'cores': 4,
                              'gpus': 0})

    pmgr = rp.PilotManager(session=rpsession)
    pilot = pmgr.submit_pilots(pd)

    tmgr = rp.TaskManager(session=rpsession)
    tmgr.add_pilots(pilot)

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
    # Does the RP Task callback not get triggered for cancellations?
    assert wrapper.cancelled()
    assert task.state == rp.states.CANCELED


@with_radical_only
@pytest.mark.asyncio
async def test_rp_future_propagate_cancel(rpsession):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp

    pd = rp.PilotDescription({'resource': 'local.localhost',
                              'cores': 4,
                              'gpus': 0})

    pmgr = rp.PilotManager(session=rpsession)
    pilot = pmgr.submit_pilots(pd)

    tmgr = rp.TaskManager(session=rpsession)
    tmgr.add_pilots(pilot)

    # Test propagation of asyncio cancellation behavior.
    td = rp.TaskDescription({'executable': '/bin/bash',
                             'arguments': ['-c', '/bin/sleep 5 && /bin/echo success'],
                             'cpu_processes': 1})
    task: rp.Task = tmgr.submit_tasks(td)

    wrapper: asyncio.Task = await scalems.radical.rp_task(task)

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


@with_radical_only
@pytest.mark.asyncio
async def test_rp_future(rpsession):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp

    pd = rp.PilotDescription({'resource': 'local.localhost',
                              'cores': 4,
                              'gpus': 0})

    pmgr = rp.PilotManager(session=rpsession)
    pilot = pmgr.submit_pilots(pd)

    tmgr = rp.TaskManager(session=rpsession)
    tmgr.add_pilots(pilot)

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


def test_rp_scalems_environment_preparation_local(rpsession):
    """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.

    This test function specifically tests the local.localhost resource.

    Note that we cannot wait on the environment preparation directly, but we can define
    a task with ``named_env`` matching the *prepare_env* key to implicitly depend on
    successful creation.
    """
    ...


@with_radical_only
def test_staging(sdist):
    """Confirm that we are able to bundle and install the package currently being tested."""
    # Use the `sdist` fixture to bundle the current package.
    # Copy the sdist archive to the RP target resource.
    # Create through an RP task that includes the sdist as input staging data.
    # Unpack and install the sdist.
    # Confirm matching versions.
    ...


@with_docker_only
def test_rp_scalems_environment_preparation_remote_docker(rpsession):
    """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.

    This test function specifically tests ssh-based dispatching to a resource
    other than local.localhost. We will use a mongodb and sshd running in Docker.
    """
    ...


@with_radical_only
def test_rp_raptor_local(rpsession):
    """Test the core RADICAL Pilot functionality that we rely on using local.localhost.

    Use sample 'master' and 'worker' scripts for to exercise the RP "raptor" features.
    """
    import radical.pilot as rp

    # define a pilot and launch it
    pd = rp.PilotDescription(
        {'resource': 'local.localhost',
         'cores': 4,
         'gpus': 0})

    pmgr = rp.PilotManager(session=rpsession)
    tmgr = rp.TaskManager(session=rpsession)
    pilot = pmgr.submit_pilots(pd)
    tmgr.add_pilots(pilot)

    # TODO: How can we recover successful workflow stages from previous failed Sessions?
    #
    # The client needs to note the sandbox locations from runs. SCALEMS can
    # then manage / maintain state tracking or state discovery to optimize workflow recovery.
    # Resumed workflows can make reference to sandboxes from previous sessions
    # (RCT work in progress: https://github.com/radical-cybertools/radical.pilot/tree/feature/sandboxes
    # slated for merge in 2021 Q2 to support `sandbox://` URIs).

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


# @with_radical_only
# @with_docker_only
# def test_rp_raptor_remote_docker(sdist, rpsession):
#     """Test the core RADICAL Pilot functionality that we rely on through ssh-based execution."""
#     import radical.pilot as rp
#
#     # define a pilot and launch it
#     pd    = rp.PilotDescription(
#                {'resource': 'docker.compute',
#                 'cores'   : 4,
#                 'gpus'    : 0})
#
#     pmgr  = rp.PilotManager(session=rpsession)
#     tmgr  = rp.TaskManager(session=rpsession)
#     pilot = pmgr.submit_pilots(pd)
#     tmgr.add_pilots(pilot)
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


# ------------------------------------------------------------------------------
#
# Note: radical.pilot.Session creation causes several deprecation warnings.
# Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
#
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@pytest.mark.asyncio
@with_radical_only
async def test_exec_rp():
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
