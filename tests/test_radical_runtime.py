"""Test the scalems.radical.runtime module."""
import asyncio
import logging
import warnings

import pytest
import radical.pilot as rp
import radical.utils as ru

import scalems.radical.executor
import scalems.radical.runtime_configuration
import scalems.radical.manager
import scalems.radical.runtime
import scalems.radical.session
from scalems.exceptions import APIError
import scalems.radical.runtime
from scalems.radical.exceptions import RPConfigurationError
from scalems.radical.session import RuntimeSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.exhaustive
@pytest.mark.asyncio
async def test_runtime_mismatch(pilot_description, event_loop, rp_configuration):
    """Make sure we catch some invalid configurations and still shut down cleanly."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        rp_session = await event_loop.run_in_executor(None, rp.Session)

        with rp_session:
            original_pmgr = await event_loop.run_in_executor(None, rp.PilotManager, rp_session)
            pilot = await event_loop.run_in_executor(
                None, original_pmgr.submit_pilots, rp.PilotDescription(pilot_description)
            )
            original_tmgr = await event_loop.run_in_executor(None, rp.TaskManager, rp_session)
            original_tmgr.add_pilots(pilot)

        assert rp_session.closed
        # This assertion may not be true:
        # assert pilot.state in rp.FINAL
        # Note that Pilot and other components may still be shutting down, but the
        # intention is that, from this point, pmgr, pilot, and tmgr are now "stale".

        rp_session = await event_loop.run_in_executor(None, rp.Session)

        with rp_session:
            runtime_session = RuntimeSession(session=rp_session, loop=event_loop, configuration=rp_configuration)

            with pytest.raises(APIError):
                runtime_session.task_manager(original_tmgr)
            original_tmgr.close()

            tmgr = rp.TaskManager(session=rp_session)
            runtime_session.task_manager(tmgr)

            with pytest.raises(APIError):
                runtime_session.pilot_manager(original_pmgr)
            original_pmgr.close()

            pmgr = rp.PilotManager(session=rp_session)
            runtime_session.pilot_manager(pmgr)

            new_pilot = runtime_session.pilot()
            assert pilot.uid != new_pilot.uid

            runtime_session.close()

            # Even here, the old Pilot may still be in 'PMGR_ACTIVE_PENDING'
            if pilot.state not in rp.FINAL:
                pilot.cancel()
            tmgr.close()
            pmgr.close()
        assert rp_session.closed


@pytest.mark.asyncio
async def test_raptor_cpi(rp_venv, pilot_description):
    """Test the Raptor management and implemented CPI calls."""
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
    runtime_config = scalems.radical.runtime_configuration.configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=True,
    )

    workflow = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(workflow, close_on_exit=True):
        async with scalems.radical.manager.launch(
            workflow_manager=workflow, runtime_configuration=runtime_config
        ) as runtime_manager:
            rm_info: dict = await runtime_manager.runtime_session.resources
            assert rm_info["requested_cores"] >= pilot_description.cores
            # Get a CPI session (start a Raptor task).
            cpi = await runtime_manager.get_cpi_session()
            assert cpi is not None
            assert cpi in runtime_manager._cpi_runners
            # Allocate resource for an MPI task (provision a Worker).
            # Submit a simple task.
            # Get the task results.
            # Finalize the session and deallocate resources.


@pytest.mark.asyncio
async def test_runtime_context_management(rp_venv, pilot_description):
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
    runtime_config = scalems.radical.runtime_configuration.configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=True,
    )

    workflow = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(workflow, close_on_exit=True):
        async with scalems.radical.manager.launch(workflow_manager=workflow, runtime_configuration=runtime_config):
            # Test for clean shutdown in trivial case.
            ...
        async with scalems.radical.manager.launch(
            workflow_manager=workflow, runtime_configuration=runtime_config
        ) as runtime_manager:
            rm_info: dict = await runtime_manager.runtime_session.resources
            assert rm_info["requested_cores"] >= pilot_description.cores
            # Get a non-raptor (CLI executable) context.
            executor = await scalems.radical.executor.provision_executor(
                runtime_manager,
                worker_requirements=None,
                task_requirements={"ranks": 2},
            )
            await asyncio.to_thread(executor.shutdown, wait=True, cancel_futures=True)

            # TODO(#335) Actually respect worker_requirements and task_requirements.
            # Test trivial raptor case
            async with scalems.radical.executor.executor(
                runtime_manager, worker_requirements=[{"ranks": 1}], task_requirements={"ranks": 1}
            ):
                ...

            # Test resource allocation errors
            with pytest.raises(RPConfigurationError):
                async with scalems.radical.executor.executor(
                    runtime_manager, worker_requirements=[{}], task_requirements={"ranks": 2}
                ):
                    ...
            with pytest.raises(RPConfigurationError):
                # Doesn't account for 1 core for the Raptor master
                async with scalems.radical.executor.executor(
                    runtime_manager, worker_requirements=None, task_requirements={"ranks": rm_info["requested_cores"]}
                ):
                    ...
            with pytest.raises(RPConfigurationError):
                # Doesn't account for 1 core for the Raptor master
                async with scalems.radical.executor.executor(
                    runtime_manager, worker_requirements=[{"ranks": rm_info["requested_cores"]}], task_requirements={}
                ):
                    ...

            # TODO(#335) Provision multiple workers
            # with pytest.raises(scalems.exceptions.DispatchError):
            #     # Note that one core is needed for the master, so we should only
            #     # be able to request N-1 cores for workers.
            #     async with scalems.radical.executor.executor(
            #         runtime_manager,
            #         worker_requirements=[{"ranks": 1}] * pilot_description.cores,
            #         task_requirements={"ranks": 1},
            #     ):
            #         ...

            # The last successful Raptor session above will probably still be finishing,
            # so we expect this next call to exercise the predicated condition in
            # RuntimeManager.acquire()
            logger.debug("Entering final executor scope.")
            async with scalems.radical.executor.executor(
                runtime_manager,
                worker_requirements=[{"ranks": rm_info["requested_cores"] - 1}],
                task_requirements={"ranks": 2},
            ):
                ...
                logger.debug("Leaving final executor scope.")
            # Raptor task may still be running at this point, since the scalems context manager
            # calls shutdown with `wait=False`.
            # TODO: We should not let runtime_manager shut down, until the dependent executors are shut down.
            logger.debug("Leaving RuntimeManager context.")

        logger.debug("Left RuntimeManager context.")
        assert not executor._queue_runner_task.is_alive()
        assert executor._work_queue.empty()
