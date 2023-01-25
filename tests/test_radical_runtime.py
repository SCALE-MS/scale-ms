"""Test the scalems.radical.Runtime state object."""
import logging
import typing
import warnings

import pytest
import radical.pilot as rp

from scalems.exceptions import APIError
from scalems.radical.runtime import Runtime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.exhaustive
def test_runtime_normal_instance(rp_task_manager, pilot_description):
    """Set the Runtime.pilot from an rp.Pilot instance."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        session: rp.Session = rp_task_manager.session

        state = Runtime(session=session)

        state.task_manager(rp_task_manager)

        # We only expect one pilot
        pilot: rp.Pilot = rp_task_manager.get_pilots()[0]
        # We get a dictionary...
        # assert isinstance(pilot, rp.Pilot)
        # But it looks like it has the pilot id in it.
        pilot_uid = typing.cast(dict, pilot)["uid"]
        pmgr_uid = typing.cast(dict, pilot)["pmgr"]
        pmgr: rp.PilotManager = session.get_pilot_managers(pmgr_uids=pmgr_uid)
        assert isinstance(pmgr, rp.PilotManager)

        state.pilot_manager(pmgr)

        pilot = pmgr.get_pilots(uids=pilot_uid)
        assert isinstance(pilot, rp.Pilot)
        state.pilot(pilot)


@pytest.mark.exhaustive
def test_runtime_normal_uid(rp_task_manager, pilot_description):
    """Set the Runtime.pilot from the UID obtained from the task_manager."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        session: rp.Session = rp_task_manager.session

        state = Runtime(session=session)

        state.task_manager(rp_task_manager.uid)

        # We only expect one pilot
        pilot: rp.Pilot = rp_task_manager.get_pilots()[0]
        # We get a dictionary...
        # assert isinstance(pilot, rp.Pilot)
        # But it looks like it has the pilot id in it.
        pilot_uid = typing.cast(dict, pilot)["uid"]

        # It is an error to set a Pilot before the PilotManager has been set.
        with pytest.raises(APIError):
            state.pilot(pilot_uid)

        pmgr_uid = typing.cast(dict, pilot)["pmgr"]

        state.pilot_manager(pmgr_uid)

        state.pilot(pilot_uid)


@pytest.mark.exhaustive
def test_runtime_bad_uid(pilot_description):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        session = rp.Session()

        with session:
            state = Runtime(session=session)

            with pytest.raises(ValueError):
                state.task_manager("spam")

            tmgr = rp.TaskManager(session=session)
            state.task_manager(tmgr)

            with pytest.raises(ValueError):
                state.pilot_manager("spam")

            pmgr = rp.PilotManager(session=session)
            state.pilot_manager(pmgr)

            with pytest.raises(ValueError):
                state.pilot_manager("spam")

            tmgr.close()
            pmgr.close()

        assert session.closed


@pytest.mark.exhaustive
def test_runtime_mismatch(pilot_description):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

        session = rp.Session()

        with session:
            original_pmgr = rp.PilotManager(session=session)
            pilot = original_pmgr.submit_pilots(rp.PilotDescription(pilot_description))
            original_tmgr = rp.TaskManager(session=session)
            original_tmgr.add_pilots(pilot)

        assert session.closed
        # This assertion may not be true:
        # assert pilot.state in rp.FINAL
        # Note that Pilot and other components may still be shutting down, but the
        # intention is that, from this point, pmgr, pilot, and tmgr are now "stale".

        session = rp.Session()

        with session:
            state = Runtime(session=session)

            with pytest.raises(APIError):
                state.task_manager(original_tmgr)
            original_tmgr.close()

            tmgr = rp.TaskManager(session=session)
            state.task_manager(tmgr)

            with pytest.raises(APIError):
                state.pilot_manager(original_pmgr)
            original_pmgr.close()

            pmgr = rp.PilotManager(session=session)
            state.pilot_manager(pmgr)

            # The UID will not resolve in the stored PilotManager.
            with pytest.raises(ValueError):
                state.pilot(pilot.uid)

            # The Pilot is detectably invalid.
            with pytest.raises(APIError):
                state.pilot(pilot)

            # Even here, the old Pilot may still be in 'PMGR_ACTIVE_PENDING'
            if pilot.state not in rp.FINAL:
                pilot.cancel()
            tmgr.close()
            pmgr.close()
        assert session.closed
