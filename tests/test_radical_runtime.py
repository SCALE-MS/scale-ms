"""Test the scalems.radical.Runtime state object."""
import asyncio
import logging
import warnings

import pytest
import radical.pilot as rp

import scalems.radical.runtime
from scalems.exceptions import APIError
from scalems.radical.runtime import RuntimeSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.asyncio
async def test_pilot_resources(rp_runtime: scalems.radical.runtime.RuntimeSession, event_loop):
    rm_info = await asyncio.wait_for(rp_runtime.resources, timeout=180)
    assert rm_info.get("requested_cores", 0) >= 1


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
