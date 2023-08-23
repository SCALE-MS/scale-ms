"""Execute through RADICAL Pilot.

For testing purposes, the RP dispatching can use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.
Refer to the repository directories ``docker`` and ``.github/workflows``.

Note: ``export RADICAL_LOG_LVL=DEBUG`` to enable RP debugging output.

**A note on SIGINT:**
We use *exit_on_error=False* in the PilotDescription to avoid RP injecting a
SIGINT in the interpreter process.
"""

import asyncio
import concurrent.futures
import dataclasses
import os
import typing
from subprocess import CompletedProcess
from subprocess import run as subprocess_run

import packaging.version
import pytest

import scalems
import scalems.call
import scalems.context
import scalems.cpi
import scalems.execution
import scalems.identifiers
import scalems.messages
import scalems.radical.executor
import scalems.radical.runtime_configuration
import scalems.radical.manager
import scalems.radical.task
import scalems.workflow

if typing.TYPE_CHECKING:
    import radical.utils as ru
    import mpi4py.MPI

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
logging.getLogger("asyncio").setLevel(logging.DEBUG)

pytestmark = pytest.mark.skipif(condition=rp is None, reason="These tests require RADICAL Pilot.")

client_scalems_version = packaging.version.Version(scalems.__version__)
if client_scalems_version.is_prerelease:
    minimum_scalems_version = client_scalems_version.public
else:
    minimum_scalems_version = client_scalems_version.base_version


def sample_callable(prefix: str, *args):
    """Compose a string."""
    result = f"{prefix}: " + ", ".join(str(item) for item in args)
    return result


def sample_mpi_callable_signature1(prefix: str, *args, comm: "mpi4py.MPI.Comm"):
    """Compose a string."""
    list_of_tuples = comm.allgather(args)
    result = f"{prefix}: " + ", ".join(str(item) for items in list_of_tuples for item in items)
    return result


def sample_mpi_callable_signature2(_comm: "mpi4py.MPI.Comm", prefix: str, *args):
    """Compose a string."""
    list_of_tuples = _comm.allgather(args)
    result = f"{prefix}: " + ", ".join(str(item) for items in list_of_tuples for item in items)
    return result


@pytest.mark.asyncio
async def test_rp_future(rp_runtime):
    """Check our Future implementation for RP at a low level.

    Fulfill the asyncio.Future protocol for a rp.Task wrapper object. The wrapper
    should appropriately yield when the rp.Task is not finished.
    """
    timeout = 120

    tmgr = rp_runtime.task_manager()

    task_description_dict = dict(
        executable="/bin/bash",
        arguments=["-c", "/bin/sleep 5 && /bin/echo success"],
        cpu_processes=1,
    )
    task_description = rp.TaskDescription(from_dict=task_description_dict)
    task_description.uid = "test-rp-future-rp-cancellation-propagation"

    # Test propagation of RP cancellation behavior
    task: rp.Task = tmgr.submit_tasks(task_description)

    rp_future: asyncio.Future = await scalems.radical.task.rp_task(task)

    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(rp_future, timeout=timeout)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e

    assert rp_future.cancelled()
    assert task.state == rp.states.CANCELED

    # Test propagation of asyncio watcher task cancellation.
    task_description.uid = "test-rp-future-asyncio-cancellation-propagation"
    task: rp.Task = tmgr.submit_tasks(task_description)

    rp_future: asyncio.Task = await scalems.radical.task.rp_task(task)

    assert isinstance(rp_future, asyncio.Task)
    rp_future.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(rp_future, timeout=5)
    except asyncio.TimeoutError as e:
        # Useful point to insert an easy debugging break point
        raise e
    assert rp_future.cancelled()

    # WARNING: rp.Task.wait blocks, and may never complete. Don't do it in the event loop thread.
    # PyCharm 2023.2 seems to have some bugs with optional arguments
    # noinspection PyTypeChecker
    watcher = asyncio.create_task(asyncio.to_thread(task.wait, timeout=timeout), name=f"watch_{task.uid}")
    try:
        state = await asyncio.wait_for(watcher, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.exception(f"Waited more than {timeout} for {watcher}")
        watcher.cancel("Canceled after waiting too long.")
        raise e
    else:
        assert state in rp.states.FINAL
    # Note that if the test is paused by a debugger, the rp task may
    # have a chance to complete before being canceled.
    # Ordinarily, that will not happen in this test.
    # assert task.state in (rp.states.CANCELED, rp.states.DONE)
    assert task.state in (rp.states.CANCELED,)

    # Test run to completion
    task_description.uid = "test-rp-future-completion"
    watcher = asyncio.create_task(asyncio.to_thread(tmgr.submit_tasks, task_description), name="rp_submit")
    try:
        task: rp.Task = await asyncio.wait_for(watcher, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.exception(f"Waited more than {timeout} to submit {task_description}.")
        watcher.cancel()
        raise e

    rp_future: asyncio.Task[rp.Task] = await scalems.radical.task.rp_task(task)
    try:
        result: rp.Task = await asyncio.wait_for(rp_future, timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.debug(f"Waited more than {timeout} for {rp_future}: {e}")
        rp_future.cancel("Canceled after waiting too long.")
        raise e
    assert task.exit_code == 0
    assert "success" in task.stdout

    assert hasattr(result, "stdout")
    assert "success" in result.stdout
    assert rp_future.done()

    # Test failure handling
    # TODO: Use a separate test for results and error handling.


@pytest.mark.asyncio
async def test_rp_submit(rp_venv, pilot_description):
    """Check the canonical low-level interface for submitting scalems tasks.

    TODO: Generalize and move to a separate file for testing all backends.

    """
    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip("This test requires a user-provided static RP venv.")

    job_endpoint: ru.Url = rp.utils.misc.get_resource_job_url(
        pilot_description.resource, pilot_description.access_schema
    )
    launch_method = job_endpoint.scheme
    if launch_method == "fork":
        pytest.skip("Raptor is not fully supported with 'fork'-based launch methods.")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    # Configure execution module.
    runtime_config = scalems.radical.runtime_configuration.RuntimeConfiguration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=True,
    )

    workflow_manager = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(workflow_manager, close_on_exit=True):
        async with scalems.radical.manager.launch(
            workflow_manager=workflow_manager, runtime_configuration=runtime_config
        ) as runtime_context:
            # Get a simple raptor context.
            async with scalems.radical.executor.executor(
                runtime_context, worker_requirements=[{}], task_requirements={}
            ) as executor:
                task1: concurrent.futures.Future = executor.submit(sample_callable, "A", 42)
            #     coro = typing.cast(Coroutine, loop.run_in_executor(executor, sample_callable, "B", 3.14))
            #     task2: asyncio.Task = asyncio.create_task(coro)
            # # Get an MPI raptor context.
            # async with scalems.radical.executor.executor(
            #     runtime_context, worker_requirements=[{}], task_requirements={"ranks": 2}
            # ):
            #     task3: asyncio.Task = scalems.submit(sample_mpi_callable_signature1, "C", "spam", comm=None)
            #     task4: asyncio.Task = scalems.submit(sample_mpi_callable_signature2, "D", "spam")
            # # Get a multi-worker MPI context. Note that we need at least N+1 ranks!
            # async with scalems.radical.executor.executor(
            #     runtime_context, worker_requirements=[{}, {}], task_requirements={"ranks": 2}
            # ):
            #     task5: asyncio.Task = scalems.submit(sample_mpi_callable_signature2, "E", "spam")
            # # Get a non-raptor (CLI executable) context.
            # async with scalems.radical.executor.executor(
            #     runtime_context, worker_requirements=None, task_requirements={"ranks": 2}
            # ):
            #     task6: asyncio.Task = scalems.executable(("/bin/echo", "spam"))
            assert task1.result() == "A: 42"
        # TODO: Refine resource lifetime management and check ScopeErrors.


@pytest.mark.asyncio
async def test_raptor_master(pilot_description, rp_venv):
    """Check our ability to launch and interact with a Master task."""
    import radical.pilot as rp

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip("This test requires a user-provided static RP venv.")

    job_endpoint: ru.Url = rp.utils.misc.get_resource_job_url(
        pilot_description.resource, pilot_description.access_schema
    )
    launch_method = job_endpoint.scheme
    if launch_method == "fork":
        pytest.skip("Raptor is not fully supported with 'fork'-based launch methods.")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    # Configure module.
    params = scalems.radical.runtime_configuration.RuntimeConfiguration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=True,
    )

    timeout = 180
    manager = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(manager, close_on_exit=True):
        async with scalems.execution.dispatch(
            manager, executor_factory=scalems.radical.executor_factory, params=params
        ) as dispatcher:
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f"test_raptor_master Session is {repr(dispatcher.runtime.session)}")

            # Bypass the scalems machinery and submit an instruction directly to the raptor task.
            # TODO: Use scalems.radical.runtime.submit()
            raptor: rp.Task = dispatcher.raptor
            assert raptor is not None and raptor.state not in rp.FINAL
            hello_command_description = rp.TaskDescription(
                from_dict={
                    "raptor_id": raptor.uid,
                    "mode": scalems.radical.raptor.CPI_MESSAGE,
                    "metadata": scalems.cpi.to_raptor_task_metadata(scalems.cpi.hello()),
                    "uid": f"command-hello-{scalems.identifiers.EphemeralIdentifier()}",
                }
            )
            # TODO: Let this be the responsibility of the submitter internals.
            if os.getenv("COVERAGE_RUN") is not None or os.getenv("SCALEMS_COVERAGE") is not None:
                hello_command_description.environment["SCALEMS_COVERAGE"] = "TRUE"
            logger.debug(f"Submitting {str(hello_command_description.as_dict())}")
            (hello_task,) = await asyncio.to_thread(
                dispatcher.runtime.task_manager().submit_tasks, [hello_command_description]
            )
            logger.debug(f"Submitted {str(hello_task.as_dict())}. Waiting...")
            hello_state = await asyncio.to_thread(hello_task.wait, state=rp.FINAL, timeout=timeout)
            logger.debug(str(hello_task.as_dict()))

            # TODO: wrap this into lifetime management.
            stop_command_description = rp.TaskDescription(
                from_dict={
                    "raptor_id": raptor.uid,
                    "mode": scalems.radical.raptor.CPI_MESSAGE,
                    "metadata": scalems.cpi.to_raptor_task_metadata(scalems.cpi.stop()),
                    "uid": f"command-stop--{scalems.identifiers.EphemeralIdentifier()}",
                }
            )
            logger.debug(f"Submitting {str(stop_command_description.as_dict())}")
            (stop_task,) = await asyncio.to_thread(
                dispatcher.runtime.task_manager().submit_tasks, [stop_command_description]
            )

            # We expect the status update -> DONE, even if self.stop() was called during result_cb for the task.
            stop_watcher = asyncio.create_task(
                asyncio.to_thread(stop_task.wait, state=rp.FINAL, timeout=timeout), name="stop-watcher"
            )

            raptor_watcher = asyncio.create_task(
                asyncio.to_thread(raptor.wait, state=rp.FINAL, timeout=timeout), name="raptor-watcher"
            )
            # If raptor task fails, stop-watcher will never complete.
            done, pending = await asyncio.wait(
                (stop_watcher, raptor_watcher), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )

            if raptor_watcher not in done:
                await asyncio.wait_for(raptor_watcher, timeout=10)
            logger.debug(f"raptor-task state: {raptor.state}")
            if raptor.state == rp.DONE and stop_watcher in pending:
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

            assert raptor.state == rp.DONE
            # END TODO

    assert hello_state == rp.DONE
    assert hello_task.stdout == repr(scalems.radical.raptor.backend_version)
    # Ref https://github.com/SCALE-MS/scale-ms/discussions/268
    assert hello_task.return_value == dataclasses.asdict(scalems.radical.raptor.backend_version)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.asyncio
async def test_worker(pilot_description, rp_venv):
    """Launch the raptor script and execute a trivial workflow."""

    if rp_venv is None:
        # Be sure to provision the venv.
        pytest.skip("This test requires a user-provided static RP venv.")

    job_endpoint: ru.Url = rp.utils.misc.get_resource_job_url(
        pilot_description.resource, pilot_description.access_schema
    )
    launch_method = job_endpoint.scheme
    if launch_method == "fork":
        pytest.skip("Raptor is not fully supported with 'fork'-based launch methods.")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    params = scalems.radical.runtime_configuration.RuntimeConfiguration(
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

    timeout = 120
    manager = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(manager, close_on_exit=True):
        assert not loop.is_closed()
        # Enter the async context manager for the default dispatcher
        async with scalems.execution.dispatch(
            manager, executor_factory=scalems.radical.executor_factory, params=params
        ) as dispatcher:
            # We have now been through RPDispatchingExecutor.runtime_startup().
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f"Session is {repr(dispatcher.runtime.session)}")

            task_uid = "add_item.scalems-test-worker"

            # Submit a raptor task
            # Bypass the scalems machinery and submit an instruction directly to the raptor task.
            # TODO: Use scalems.radical.runtime.submit()
            raptor: rp.Task = dispatcher.raptor
            assert raptor is not None and raptor.state not in rp.FINAL

            add_item_task_description = rp.TaskDescription()
            add_item_task_description.raptor_id = raptor.uid
            add_item_task_description.uid = task_uid
            add_item_task_description.cpu_processes = 1
            add_item_task_description.cpu_process_type = (rp.SERIAL,)
            add_item_task_description.mode = scalems.radical.raptor.CPI_MESSAGE
            cpi_call = scalems.cpi.add_item(work_item)
            add_item_task_description.metadata = scalems.cpi.to_raptor_task_metadata(cpi_call)

            # TODO: Let this be the responsibility of the submitter internals.
            if os.getenv("COVERAGE_RUN") is not None or os.getenv("SCALEMS_COVERAGE") is not None:
                add_item_task_description.environment["SCALEMS_COVERAGE"] = "TRUE"

            task_manager = dispatcher.runtime.task_manager()

            submitter = asyncio.create_task(
                asyncio.to_thread(task_manager.submit_tasks, add_item_task_description), name="rp_submit"
            )
            try:
                submitted_rptask: rp.Task = await asyncio.wait_for(submitter, timeout=timeout)
            except asyncio.TimeoutError as e:
                logger.exception(f"Waited more than {timeout} to submit {add_item_task_description}.")
                submitter.cancel()
                raise e

            cpi_command_future: asyncio.Task = await scalems.radical.task.rp_task(submitted_rptask)
            try:
                add_item_task: rp.Task = await asyncio.wait_for(cpi_command_future, timeout=timeout)
            except asyncio.TimeoutError as e:
                logger.debug(f"Waited more than {timeout} for {cpi_command_future}: {e}")
                cpi_command_future.cancel("Canceled after waiting too long.")
                raise e
            assert add_item_task.exit_code == 0

            # Ref https://github.com/SCALE-MS/scale-ms/discussions/268
            try:
                rp_release = packaging.version.parse(rp.version_detail).release
            except packaging.version.InvalidVersion:
                # Ref: https://github.com/radical-cybertools/radical.pilot/issues/2807
                rp_release = None
            if rp_release is not None and rp_release[0:2] == (1, 20):
                assert add_item_task.return_value is None
                work_item_task_id = add_item_task.stdout
            else:
                assert add_item_task.return_value is not None
                work_item_task_id = add_item_task.return_value

            logger.debug(f"Master submitted task {work_item_task_id} to Worker.")
            # We now have the ID of the Task supporting the workflow item we added.
            # We can get the result from a report provided by the Master, or we can use
            # some sort of query command (both TBD).
            # TODO(#229): Check an actual data result.
            # TODO: Update the RPDispatchingExecutor to send the stop rpc call.


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.asyncio
async def test_rp_function(pilot_description, rp_venv, tmp_path):
    """Test our automation for RP Task generation for function calls."""
    # timeout = 180

    # Hopefully, this requirement is temporary.
    if rp_venv is None:
        pytest.skip("This test requires a user-provided static RP venv.")

    job_endpoint: ru.Url = rp.utils.misc.get_resource_job_url(
        pilot_description.resource, pilot_description.access_schema
    )
    launch_method = job_endpoint.scheme
    if launch_method == "fork":
        pytest.skip("Raptor is not fully supported with 'fork'-based launch methods.")

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    # Configure module.
    params = scalems.radical.runtime_configuration.RuntimeConfiguration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=True,
    )

    # Test RPDispatcher context
    manager = scalems.radical.workflow_manager(loop)

    with scalems.workflow.scope(manager, close_on_exit=True):
        assert not loop.is_closed()
        # TODO: Further simplification. E.g.
        #     call_ref = manager.submit(function_call(**sample_call_input))
        #     return_value = call_ref.result()

        async with scalems.execution.dispatch(
            manager, executor_factory=scalems.radical.executor_factory, params=params
        ) as dispatcher:
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
            # Note: the Master (and Worker) have already started, but may not be
            # commensurate with *requirements*.
            # Resource constraints:
            # * the Master uses one of the cores available to the Pilot,
            #   so it is not available to Tasks.
            # * Tasks cannot span Workers, so we need to make sure that we
            #   provision a sufficiently large Worker.
            # * raptor does not support the memory/disk task constraints.
            # * GPUs: gpus-per-rank is not well explored in raptor. deviations unknown.
            # * nodes: Workers may span nodes, so this shouldn't be a problem.
            # Other set-up details:
            # * pre_exec needs to happen on the Worker, not the Task
            #   (default scalems pre_exec is already handled this way.
            #   We can add a check that user has not extended it until we can
            #   update the Worker provisioning.).
            # * For 0th step, we can provision one Worker with all resources.
            # * For immediate follow-up: Worker provisioning needs to be delayed,
            #   and carried out with respect to the work load.

            rp_task_result: scalems.radical.task.RPTaskResult = await scalems.radical.task.subprocess_to_rp_task(
                call_handle, dispatcher=dispatcher
            )

            call_result: scalems.call.CallResult = await scalems.radical.task.wrapped_function_result_from_rp_task(
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
    params = scalems.radical.runtime_configuration.RuntimeConfiguration(
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
        async with scalems.execution.dispatch(
            manager, executor_factory=scalems.radical.executor_factory, params=params
        ) as dispatcher:
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
