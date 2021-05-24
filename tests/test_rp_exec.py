#!/usr/bin/env python3

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
import json
import logging
import os
import subprocess
import time
import typing
import urllib.parse
import warnings

import pytest

import scalems
import scalems.context
import scalems.radical
import scalems.radical.runtime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.


@pytest.mark.skip(reason='Unimplemented.')
def test_rp_static_venv(rp_venv, pilot_description):
    """Confirm that a prepared venv is usable by RP.

    .. todo:: Are we using this for the Pilot? Or just the Tasks?
    """
    # If scalems is installed and usable, and the venv is activated,
    # then the `scalems_rp_master` entry point script should be discoverable with `which`
    # and executable.
    ...


def test_rp_basic_task_local(rp_task_manager, pilot_description):
    if pilot_description.access_schema and pilot_description.access_schema != 'local':
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

    if (pilot_description.access_schema and pilot_description.access_schema == 'local') \
            or pilot_description.resource.startswith('local'):
        pytest.skip('This test is only for remote execution.')

    tmgr = rp_task_manager
    session = tmgr.session
    assert not session.closed

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


@pytest.mark.skip('Disabled pending progress on #89 and #90.')
def test_prepare_venv(rp_task_manager, sdist):
    """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.

    This test function specifically tests the local.localhost resource.

    Note that we cannot wait on the environment preparation directly, but we can define
    a task with ``named_env`` matching the *prepare_env* key to implicitly depend on
    successful creation.
    """
    # NOTE: *sdist* is a path of an sdist archive that we could stage for the venv installation.
    # QUESTION: Can't we use the radical.pilot package archive that was already placed for bootstrapping the pilot?

    # TODO: Merge with test_rp_raptor_local but use the installed scalems_rp_master and scalems_rp_worker files

    import radical.pilot as rp
    import radical.saga as rs
    import radical.utils as ru
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

    sdist_names = {
        'scalems': os.path.basename(sdist),
        'rp': rp.sdist_name,
        'ru': ru.sdist_name,
        'rs': rs.sdist_name
    }
    sdist_local_paths = {
        'scalems': sdist,
        'rp': rp.sdist_path,
        'rs': rs.sdist_path,
        'ru': ru.sdist_path
    }
    logger.debug('Checking paths: ' + ', '.join(sdist_local_paths.values()))
    for path in sdist_local_paths.values():
        assert os.path.exists(path)

    sandbox_path = urllib.parse.urlparse(pilot.pilot_sandbox).path

    sdist_session_paths = {name: os.path.join(sandbox_path, sdist_names[name]) for name in sdist_names.keys()}

    logger.debug('Staging ' + ', '.join(sdist_session_paths.values()))

    input_staging = []
    for name in sdist_names.keys():
        input_staging.append({
            'source': sdist_local_paths[name],
            'target': sdist_session_paths[name],
            'action': rp.TRANSFER
        })
    pilot.stage_in(input_staging)

    tmgr = rp_task_manager

    pilot.prepare_env({
        'scalems_env': {
            'type': 'virtualenv',
            'version': '3.8',
            'setup': list(sdist_session_paths.values())}})

    td = rp.TaskDescription({'executable': 'python3',
                             'arguments': ['-c',
                                           'import radical.pilot as rp;'
                                           'import scalems;'
                                           'print(rp.version_detail);'
                                           'print(scalems.__file__)'],
                             'named_env': 'scalems_env'})
    task = tmgr.submit_tasks(td)
    tmgr.wait_tasks()
    logger.debug(f'RP version details and scalems location: {task.stdout}')
    assert task.exit_code == 0


@pytest.mark.asyncio
async def test_rp_future(rp_task_manager):
    """Check our Future implementation.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    import radical.pilot as rp

    tmgr = rp_task_manager

    td = rp.TaskDescription({'executable': '/bin/bash',
                             'arguments': ['-c', '/bin/sleep 5 && /bin/echo success'],
                             'cpu_processes': 1})

    # Test propagation of RP cancellation behavior
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Future = await scalems.radical.rp_task(task, future)

    task.cancel()
    try:
        # TODO: With Python 3.9, check cancellation message for how the cancellation propagated.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(wrapper, timeout=120)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e

    assert future.cancelled()
    assert wrapper.cancelled()
    assert task.state == rp.states.CANCELED

    # Test propagation of asyncio watcher task cancellation.
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Task = await scalems.radical.rp_task(task, future)

    assert isinstance(wrapper, asyncio.Task)
    wrapper.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(wrapper, timeout=5)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e
    assert wrapper.cancelled()
    assert future.cancelled()

    # WARNING: rp.Task.wait() never completes with no arguments.
    # WARNING: This blocks. Don't do it in the event loop thread.
    task.wait(state=rp.states.CANCELED, timeout=120)
    # Note that if the test is paused by a debugger, the rp task may
    # have a chance to complete before being canceled.
    # Ordinarily, that will not happen in this test.
    # assert task.state in (rp.states.CANCELED, rp.states.DONE)
    assert task.state in (rp.states.CANCELED,)

    # Test propagation of asyncio future cancellation.
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Task = await scalems.radical.rp_task(task, future)

    assert isinstance(wrapper, asyncio.Task)
    future.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(future, timeout=5)
        await asyncio.wait_for(wrapper, timeout=1)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e
    assert not wrapper.cancelled()
    assert future.cancelled()

    # WARNING: rp.Task.wait() never completes with no arguments.
    # WARNING: This blocks. Don't do it in the event loop thread.
    task.wait(state=rp.states.CANCELED, timeout=120)
    # Note that if the test is paused by a debugger, the rp task may
    # have a chance to complete before being canceled.
    # Ordinarily, that will not happen in this test.
    # assert task.state in (rp.states.CANCELED, rp.states.DONE)
    assert task.state in (rp.states.CANCELED,)

    # Test run to completion
    task: rp.Task = tmgr.submit_tasks(td)

    future = asyncio.get_running_loop().create_future()
    wrapper: asyncio.Task = await scalems.radical.rp_task(task, future)

    timeout = 120
    try:
        result = await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.debug(f'Waited more than {timeout} for {future}: {e}')
        result = None
    assert task.exit_code == 0
    assert 'success' in task.stdout

    assert 'stdout' in result
    assert 'success' in result['stdout']
    assert wrapper.done()

    # Test failure handling
    # TODO: Use a separate test for results and error handling.


# @pytest.mark.skip(reason='Unimplemented.')
# def test_staging(sdist, rp_task_manager):
#     """Confirm that we are able to bundle and install the package currently being tested."""
#     # Use the `sdist` fixture to bundle the current package.
#     # Copy the sdist archive to the RP target resource.
#     # Create through an RP task that includes the sdist as input staging data.
#     # Unpack and install the sdist.
#     # Confirm matching versions.
#     # TODO: Test both with and without a provided config file.
#     assert False


# @pytest.mark.skip(reason='Unimplemented.')
# def test_rp_scalems_environment_preparation_remote_docker(rp_task_manager):
#     """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.
#
#     This test function specifically tests ssh-based dispatching to a resource
#     other than local.localhost. We will use a mongodb and sshd running in Docker.
#     """
#     assert False


# @pytest.mark.skipif(condition=bool(os.getenv('CI')), reason='Skipping slow test in CI environment.')
@pytest.mark.skip(reason='Test disabled pending RCT bug fix. See issue #119.')
def test_rp_raptor_staging(pilot_description, rp_venv):
    """Test file staging for raptor Master and Worker tasks.

    - upon pilot startup, transfer a file to the pilot sandbox
    - upon master startup, create a link to that file for each master
    - for each task, copy the file into the task sandbox
    - upon task completion, transfer the files to the client (and rename them)
    """
    import time
    import radical.pilot as rp

    # Note: we need to install the current scalems package to test remotely.
    # If this is problematic, we can add a check like the following.
    #     if pilot_description.resource != 'local.localhost' \
    #             and pilot_description.access_schema \
    #             and pilot_description.access_schema != 'local':
    #         pytest.skip('This test is only for local execution.')

    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=DeprecationWarning)
        session = rp.Session()
    fname = '%d.dat' % os.getpid()
    fpath = os.path.join('/tmp', fname)
    data: str = time.asctime()

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip('This test requires a user-provided static RP venv.')

    if rp_venv:
        pre_exec = ['. {}/bin/activate'.format(rp_venv)]
    else:
        pre_exec = None

    try:
        pmgr = rp.PilotManager(session=session)
        tmgr = rp.TaskManager(session=session)

        # Illustrate data staging as part of the Pilot launch.
        # By default, file is copied to the root of the Pilot sandbox,
        # where it can be referenced as 'pilot:///filename'
        # Alternatively: pilot.stage_in() and pilot.stage_output() (blocking calls)
        pilot_description.exit_on_error = True
        pilot_description.input_staging = [fpath]
        with open(fpath, 'w') as fh:
            fh.writelines([data])
        try:
            pilot = pmgr.submit_pilots(pilot_description)
            # Confirmation that the input file has been staged by waiting for pilot state.
            pilot.wait(state=[rp.states.PMGR_ACTIVE] + rp.FINAL)
        finally:
            os.unlink(fpath)

        tmgr.add_pilots(pilot)

        uid = 'scalems.master.001'
        # Illustrate another mode of data staging with the Master task submission.
        td = rp.TaskDescription(
            {
                'uid': uid,
                'executable': 'scalems_rp_master',
                'input_staging': [{'source': 'pilot:///%s' % fname,
                                   'target': 'pilot:///%s.%s.lnk' % (fname, uid),
                                   'action': rp.LINK}],
                'pre_exec': pre_exec
                # 'named_env': 'scalems_env'
            }
        )

        master = tmgr.submit_tasks(td)

        # Illustrate availability of scheduler and of data staged with Master task.
        # When the task enters AGENT_SCHEDULING_PENDING it has passed all input staging,
        # and the files will be available.
        # (see https://docs.google.com/drawings/d/1q5ehxIVdln5tXEn34mJyWAmxBk_DqZ5wwkl3En-t5jo/)

        # Confirm that Master script is running (and ready to receive raptor tasks)
        # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not check type.
        master.wait(state=[rp.states.AGENT_EXECUTING] + rp.FINAL)
        assert master.state not in {rp.CANCELED, rp.FAILED}

        # define raptor tasks and submit them to the master
        tds = list()
        # Illustrate data staging as part of raptor task submission.
        # Note that tasks submitted by the client
        # a sandboxed task directory, whereas those submitted by the Master (through Master.request(),
        # through the wrapper script or the Master.create_initial_tasks() hook) do not,
        # and do not have a data staging phase.
        for i in range(3):
            uid = 'scalems.%06d' % i
            work = {'mode': 'call',
                    'cores': 1,
                    'timeout': 10,  # seconds
                    'data': {'method': 'hello',
                             'kwargs': {'world': uid}}}
            tds.append(rp.TaskDescription({
                'uid': uid,
                'executable': '-',
                'input_staging': [{'source': 'pilot:///%s.%s.lnk' % (fname, master.uid),
                                   'target': 'task:///%s' % fname,
                                   'action': rp.COPY}],
                'output_staging': [{'source': 'task:///%s' % fname,
                                    'target': 'client:///%s.%s.out' % (fname, uid),
                                    'action': rp.TRANSFER}],
                'scheduler': master.uid,
                'arguments': [json.dumps(work)],
                'pre_exec': pre_exec
            }))
        # TODO: Organize client-side data with managed hierarchical paths.
        # Question: RP maintains a filesystem hierarchy on the client side, correct?
        # Answer: only for profiling and such: do not use for data or user-facing stuff.
        tasks = tmgr.submit_tasks(tds)
        # TODO: Clarify the points at which the data exists or is accessed.
        # * When the (client-submitted) task enters AGENT_STAGING_OUTPUT_PENDING,
        #   it has finished executing and output data should be accessible as 'task:///outfile'.
        # * When the (client-submitted) task reaches one of the rp.FINAL stages, it has finished
        #   output staging and files are accessible at the location specified in 'output_staging'.
        # * Tasks submitted directly by the Master (example?) do not perform output staging;
        #   data is written before entering Master.result_cb().
        # RP Issue: client-submitted Tasks need to be accessible through a path that is common
        # with the Master-submitted (`request()`) tasks. (SCALE-MS #108)

        assert len(tasks) == len(tds)
        # 'arguments' (element 0) gets wrapped in a Request at the Master by _receive_tasks,
        # then the list of requests is passed to Master.request(), which is presumably
        # an extension point for derived Master implementations. The base class method
        # converts requests to dictionaries and adds them to a request queue, from which they are
        # picked up by the Worker in _request_cb. Then picked up in forked interpreter
        # by Worker._dispatch, which checks the *mode* of the Request and dispatches
        # according to native or registered mode implementations. (e.g. 'call' (native) or 'scalems')

        # task process is launched with Python multiprocessing (native) module and added to self._pool.
        # When the task runs, it's result triggers _result_cb

        # wait for *those* tasks to complete and report results
        tmgr.wait_tasks(uids=[t.uid for t in tasks])

        # Cancel the master.
        tmgr.cancel_tasks(uids=master.uid)
        # Cancel blocks until the task is done so the following wait it currently redundant,
        # but there is a ticket open to change this behavior.
        # See https://github.com/radical-cybertools/radical.pilot/issues/2336
        tmgr.wait_tasks()

        # Note that these map as follows:
        #     * 'client:///' == $PWD
        #     * 'task:///' == urllib.parse.urlparse(task.sandbox).path
        #     * 'pilot:///' == urllib.parse.urlparse(pilot.pilot_sandbox).path

        for t in tasks:
            print(t)
            outfile = './%s.%s.out' % (fname, t.uid)
            assert os.path.exists(outfile)
            with open(outfile, 'r') as outfh:
                assert outfh.readline().rstrip() == data
            os.unlink(outfile)

        pilot.cancel()
        tmgr.close()
        pmgr.close()

    finally:
        session.close(download=False)


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
    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip('This test requires a user-provided static RP venv.')

    original_context = scalems.context.get_context()
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

    with scalems.context.scope(manager):
        assert not loop.is_closed()
        # Enter the async context manager for the default dispatcher
        cmd1 = scalems.executable(('/bin/echo',))
        async with manager.dispatch(params=params):
            cmd2 = scalems.executable(('/bin/echo', 'hello', 'world'))
            # TODO: Clarify whether/how result() method should work in this scope.
            # TODO: Make scalems.wait(cmd) work as expected in this scope.
        assert cmd1.done() and cmd2.done()
        logger.debug(cmd1.result())
        logger.debug(cmd2.result())

    # TODO: Output typing.
    out1: dict = cmd1.result()
    for output in out1['description']['output_staging']:
        assert os.path.exists(output['target'])
    out2: dict = cmd2.result()
    for output in out2['description']['output_staging']:
        assert os.path.exists(output['target'])
        if output['target'].endswith('stdout'):
            with open(output['target'], 'r') as fh:
                line = fh.readline()
                assert line.rstrip() == 'hello world'

    # Test active context scoping.
    assert scalems.context.get_context() is original_context
    assert not loop.is_closed()


@pytest.mark.skip(reason='Unimplemented.')
@pytest.mark.asyncio
async def test_batch():
    """Run a batch of uncoupled tasks, dispatched through RP."""
    assert False


@pytest.mark.skip(reason='Unimplemented.')
@pytest.mark.asyncio
async def test_chained_commands():
    """Run a sequence of two tasks with a data flow dependency."""
    assert False


@pytest.mark.skip(reason='Unimplemented.')
@pytest.mark.asyncio
async def test_file_staging():
    """Test a simple SCALE-MS style command chain that places a file and then retrieves it."""
    assert False


if __name__ == '__main__':
    # TODO: Name the intended venv
    venv = None

    import radical.pilot as rp

    pd_init = {'resource': 'local.localhost',
               'runtime': 30,
               'cores': 1,
               }
    pdesc = rp.PilotDescription(pd_init)
    test_rp_raptor_staging(pdesc, venv, None)
