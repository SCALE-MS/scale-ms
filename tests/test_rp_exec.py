"""Dispatch scalems.exec through RADICAL Pilot.

For testing purposes, the RP dispatching can use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.
Refer to the repository directories ``docker`` and ``.github/workflows``.

Note: ``export RADICAL_LOG_LVL=DEBUG`` to enable RP debugging output.
"""

import asyncio
import json
import os
from subprocess import CompletedProcess
from subprocess import run as subprocess_run

import packaging.version
import pytest

import scalems
import scalems.call
import scalems.compat
import scalems.context
import scalems.messages
import scalems.workflow

try:
    import radical.pilot as rp
except ImportError:
    rp = None
else:
    import scalems.radical
    import scalems.radical.raptor
    import scalems.radical.runtime

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.

pytestmark = pytest.mark.skipif(condition=rp is None, reason="These tests require RADICAL Pilot.")

client_scalems_version = packaging.version.Version(scalems.__version__)
if client_scalems_version.is_prerelease:
    minimum_scalems_version = client_scalems_version.public
else:
    minimum_scalems_version = client_scalems_version.base_version


# We either need to mock a RP agent or gather coverage files from remote environments.
# This test does not add any coverage, and only adds measurable coverage or coverage
# granularity if we either mock a RP agent or gather coverage files from remote environments.
@pytest.mark.experimental
@pytest.mark.xfail(reason="Needs updates for shutdown behavior of rp.raptor.Master (Discussion #289)")
@pytest.mark.asyncio
async def test_raptor_master(pilot_description, rp_venv):
    """Check our ability to launch and interact with a Master task."""
    import radical.pilot as rp

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip("This test requires a user-provided static RP venv.")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    # Configure module.
    params = scalems.radical.runtime.Configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=True,
    )

    timeout = 180
    to_thread = scalems.compat.get_to_thread()

    manager = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(manager, close_on_exit=True):
        async with manager.dispatch(params=params) as dispatcher:
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f"test_raptor_master Session is {repr(dispatcher.runtime.session)}")

            # Bypass the scalems machinery and submit an instruction directly to the master task.
            scheduler: rp.Task = dispatcher.runtime.scheduler
            td = rp.TaskDescription(
                from_dict={
                    "scheduler": scheduler.uid,
                    "mode": scalems.radical.raptor.CPI_MESSAGE,
                    "metadata": scalems.messages.HelloCommand().encode(),
                    "uid": "task-hello",
                }
            )
            logger.debug(f"Submitting {str(td.as_dict())}")
            tasks = await to_thread(dispatcher.runtime.task_manager().submit_tasks, [td])
            hello_task = tasks[0]
            logger.debug(f"Submitted {str(hello_task.as_dict())}. Waiting...")
            state = await to_thread(hello_task.wait, state=rp.FINAL, timeout=timeout)
            logger.debug(str(hello_task.as_dict()))

            td.metadata = scalems.messages.StopCommand().encode()
            td.output_staging = []
            td.uid = "task-stop"
            logger.debug(f"Submitting {str(td.as_dict())}")
            tasks = await to_thread(dispatcher.runtime.task_manager().submit_tasks, [td])
            stop_task = tasks[0]

            # We expect the status update -> DONE, even if self.stop() was called during result_cb for the task.
            stop_watcher = asyncio.create_task(
                to_thread(stop_task.wait, state=rp.FINAL, timeout=timeout), name="stop-watcher"
            )

            scheduler_watcher = asyncio.create_task(
                to_thread(scheduler.wait, state=rp.FINAL, timeout=timeout), name="master-watcher"
            )
            # If master task fails, stop-watcher will never complete.
            done, pending = await asyncio.wait(
                (stop_watcher, scheduler_watcher), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )

            if scheduler_watcher not in done:
                await asyncio.wait_for(scheduler_watcher, timeout=10)
            logger.debug(f"scheduler-task state: {scheduler.state}")
            if scheduler.state == rp.DONE and stop_watcher in pending:
                # Waiting longer doesn't seem to help.
                # logger.debug("Waiting a little longer for the stop task to wrap up.")
                # await asyncio.wait_for(stop_watcher, timeout=timeout)
                # TODO(#289) Reconcile expectations regarding stop-task state updates.
                # The prescribed behavior is that the task _should_ reach final state.
                # The following assertion will alert us to the bug fix in scalems or rp that
                # is preventing expected behavior. When it ceases to be true, then we should
                # adjust our assumptions about the behavior of tasks that include
                # a `Master.stop()` in the result_cb.
                assert stop_task.state not in rp.FINAL
            if not stop_watcher.done():
                logger.debug(f"Canceling {stop_task}.")
                stop_watcher.cancel()
            logger.debug(f"stop-task state: {stop_task.state}")

            assert scheduler.state == rp.DONE

    assert state == rp.DONE
    assert hello_task.stdout == repr(scalems.radical.raptor.backend_version)
    # Ref https://github.com/SCALE-MS/scale-ms/discussions/268
    # assert task.return_value == scalems.__version__

    # Note: As we refine the dispatching protocol, make sure we don't wait for STOP controls to finish.
    #     state = await asyncio.wait_for(stop_watcher, timeout=120)
    #     assert state in rp.states.FINAL


@pytest.mark.experimental
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.asyncio
async def test_worker(pilot_description, rp_venv):
    """Launch the master script and execute a trivial workflow."""

    if rp_venv is None:
        # Be sure to provision the venv.
        pytest.skip("This test requires a user-provided static RP venv.")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    params = scalems.radical.runtime.Configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=True,
    )

    # TODO: Make the work representation non-Raptor-specific or decouple
    #  the Raptor oriented representation of Work from the client-side representation.
    work_item = scalems.radical.raptor.ScalemsRaptorWorkItem(
        func=print.__name__, module=print.__module__, args=["hello world"], kwargs={}, comm_arg_name=None
    )

    manager = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(manager, close_on_exit=True):
        assert not loop.is_closed()
        # Enter the async context manager for the default dispatcher
        async with manager.dispatch(params=params) as dispatcher:
            # We have now been through RPDispatchingExecutor.runtime_startup().
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f"Session is {repr(dispatcher.runtime.session)}")

            task_uid = "task.scalems-test-worker"
            # Bypass the scalems machinery and submit an instruction directly to the master task.
            scheduler: rp.Task = dispatcher.runtime.scheduler

            task_description = rp.TaskDescription()
            task_description.scheduler = scheduler.uid
            task_description.uid = task_uid
            task_description.cpu_processes = 1
            task_description.cpu_process_type = (rp.SERIAL,)
            task_description.mode = scalems.radical.raptor.CPI_MESSAGE
            task_description.metadata = scalems.messages.AddItem(json.dumps(work_item)).encode()

            task_manager = dispatcher.runtime.task_manager()
            to_thread = scalems.compat.get_to_thread()
            timeout = 120
            # Submit a raptor task
            # TODO: Use scalems.radical.runtime.submit()
            watcher = asyncio.create_task(to_thread(task_manager.submit_tasks, task_description), name="rp_submit")
            try:
                rp_watcher: rp.Task = await asyncio.wait_for(watcher, timeout=timeout)
            except asyncio.TimeoutError as e:
                logger.exception(f"Waited more than {timeout} to submit {task_description}.")
                watcher.cancel()
                raise e

            rp_future: asyncio.Task = await scalems.radical.runtime.rp_task(rp_watcher)
            try:
                rp_task: rp.Task = await asyncio.wait_for(rp_future, timeout=timeout)
            except asyncio.TimeoutError as e:
                logger.debug(f"Waited more than {timeout} for {rp_future}: {e}")
                rp_future.cancel("Canceled after waiting too long.")
                raise e
            assert rp_task.exit_code == 0

            # Ref https://github.com/SCALE-MS/scale-ms/discussions/268
            try:
                rp_release = packaging.version.parse(rp.version_detail).release
            except packaging.version.InvalidVersion:
                # Ref: https://github.com/radical-cybertools/radical.pilot/issues/2807
                rp_release = None
            if rp_release is not None and rp_release[0:2] == (1, 20):
                assert rp_task.return_value is None
                work_item_task_id = rp_task.stdout
            else:
                assert rp_task.return_value is not None
                work_item_task_id = rp_task.return_value

            logger.debug(f"Master submitted task {work_item_task_id} to Worker.")
            # TODO(#229): Check an actual data result.


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


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.asyncio
async def test_rp_function(pilot_description, rp_venv, tmp_path):
    """Test our automation for RP Task generation for function calls."""
    # timeout = 180

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip("This test requires a user-provided static RP venv.")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    # Configure module.
    params = scalems.radical.runtime.Configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
    )

    # Test RPDispatcher context
    manager = scalems.radical.workflow_manager(loop)

    with scalems.workflow.scope(manager, close_on_exit=True):
        assert not loop.is_closed()
        # TODO: Further simplification. E.g.
        #     call_ref = manager.submit(function_call(**sample_call_input))
        #     return_value = call_ref.result()

        async with manager.dispatch(params=params) as dispatcher:
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f"exec_rp Session is {repr(dispatcher.runtime.session)}")
            task_uid = "test_rp_function-1"

            call_handle: scalems.call._Subprocess = await scalems.call.function_call_to_subprocess(
                func=subprocess_run,
                kwargs={"args": ["/bin/echo", "hello", "world"], "capture_output": True},
                label=task_uid,
                manager=manager,
                requirements=None,
            )
            rp_task_result: scalems.radical.runtime.RPTaskResult = await scalems.radical.runtime.subprocess_to_rp_task(
                call_handle, dispatcher=dispatcher
            )

            call_result: scalems.call.CallResult = await scalems.radical.runtime.wrapped_function_result_from_rp_task(
                call_handle, rp_task_result
            )

    completed_process: CompletedProcess = call_result.return_value
    assert "hello world" in completed_process.stdout.decode(encoding="utf8")


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.asyncio
async def test_rp_executable(pilot_description, rp_venv):
    """Test our scalems.executable implementation for RADICAL Pilot."""
    import radical.pilot as rp

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip("This test requires a user-provided static RP venv.")

    original_context = scalems.workflow.get_scope()
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    # Configure module.
    params = scalems.radical.runtime.Configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
    )

    # Test RPDispatcher context
    manager = scalems.radical.workflow_manager(loop)

    with scalems.workflow.scope(manager, close_on_exit=True):
        assert not loop.is_closed()
        # Test a command from before entering the dispatch context
        cmd1 = scalems.executable(("/bin/echo", "hello", "world"), stdout="stdout1.txt")
        # Enter the async context manager for the default dispatcher
        async with manager.dispatch(params=params) as dispatcher:
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f"exec_rp Session is {repr(dispatcher.runtime.session)}")
            # Test a command issued after entering the dispatch context.
            # TODO: Check data flow dependency.
            # cmd2 = scalems.executable(("/bin/cat", cmd1.stdout), stdout="stdout2.txt")
            # TODO: Check *stdin* shim.
            # cmd2 = scalems.executable(("/bin/cat", "-"), stdin=cmd1.stdout, stdout="stdout2.txt")
            cmd2 = scalems.executable(("/bin/echo", "hello", "world"), stdout="stdout2.txt")
            # TODO: Clarify whether/how result() method should work in this scope.
            # TODO: Make scalems.wait(cmd) work as expected in this scope.
        assert cmd1.done()
        assert cmd2.done()
        logger.debug(cmd1.result())
        logger.debug(cmd2.result())

    # TODO: Output typing.
    out1: rp.Task = cmd1.result()
    for output in out1.description["output_staging"]:
        assert os.path.exists(output["target"])
    out2: rp.Task = cmd2.result()
    for output in out2.description["output_staging"]:
        assert os.path.exists(output["target"])
        if output["target"].endswith("stdout"):
            with open(output["target"], "r") as fh:
                line = fh.readline()
                assert line.rstrip() == "hello world"

    # Test active context scoping.
    assert scalems.workflow.get_scope() is original_context
    assert not loop.is_closed()


@pytest.mark.skip(reason="Unimplemented.")
@pytest.mark.asyncio
async def test_batch():
    """Run a batch of uncoupled tasks, dispatched through RP."""
    assert False
