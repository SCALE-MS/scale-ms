"""Test the scalems.radical.Runtime state object."""
import typing
import warnings

import pytest
import radical.pilot as rp

from scalems.exceptions import APIError
from scalems.radical import Runtime


def test_runtime_normal_instance(rp_task_manager, pilot_description):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.task_manager')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.db.database')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.session')

        session: rp.Session = rp_task_manager.session

        with session:
            state = Runtime(session=session)

            state.task_manager(rp_task_manager)

            # We only expect one pilot
            pilot: rp.Pilot = rp_task_manager.get_pilots()[0]
            # We get a dictionary...
            # assert isinstance(pilot, rp.Pilot)
            # But it looks like it has the pilot id in it.
            pilot_uid = typing.cast(dict, pilot)['uid']
            pmgr_uid = typing.cast(dict, pilot)['pmgr']
            pmgr: rp.PilotManager = session.get_pilot_managers(pmgr_uids=pmgr_uid)
            assert isinstance(pmgr, rp.PilotManager)

            state.pilot_manager(pmgr)

            pilot = pmgr.get_pilots(uids=pilot_uid)
            assert isinstance(pilot, rp.Pilot)
            state.pilot(pilot)


def test_runtime_normal_uid(rp_task_manager, pilot_description):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.task_manager')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.db.database')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.session')

        session: rp.Session = rp_task_manager.session

        with session:
            state = Runtime(session=session)

            state.task_manager(rp_task_manager.uid)

            # We only expect one pilot
            pilot: rp.Pilot = rp_task_manager.get_pilots()[0]
            # We get a dictionary...
            # assert isinstance(pilot, rp.Pilot)
            # But it looks like it has the pilot id in it.
            pilot_uid = typing.cast(dict, pilot)['uid']

            # It is an error to set a Pilot before the PilotManager has been set.
            with pytest.raises(APIError):
                state.pilot(pilot_uid)

            pmgr_uid = typing.cast(dict, pilot)['pmgr']

            state.pilot_manager(pmgr_uid)

            state.pilot(pilot_uid)


def test_runtime_bad_uid(pilot_description):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.task_manager')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.db.database')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.session')

        session = rp.Session()

        with session:
            state = Runtime(session=session)

            with pytest.raises(ValueError):
                state.task_manager('spam')

            tmgr = rp.TaskManager(session=session)
            state.task_manager(tmgr)

            with pytest.raises(ValueError):
                state.pilot_manager('spam')

            pmgr = rp.PilotManager(session=session)
            state.pilot_manager(pmgr)

            with pytest.raises(ValueError):
                state.pilot_manager('spam')


def test_runtime_mismatch(pilot_description):
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.task_manager')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.db.database')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.session')

        session = rp.Session()

        with session:
            pmgr = rp.PilotManager(session=session)
            pilot = pmgr.submit_pilots(rp.PilotDescription(pilot_description))
            tmgr = rp.TaskManager(session=session)
            tmgr.add_pilots(pilot)

        # pmgr, pilot, and tmgr are now stale.

        session = rp.Session()

        with session:
            state = Runtime(session=session)

            with pytest.raises(APIError):
                state.pilot_manager(pmgr)

            pmgr = rp.PilotManager(session=session)
            state.pilot_manager(pmgr)

            with pytest.raises(APIError):
                state.task_manager(tmgr)

            tmgr = rp.TaskManager(session=session)
            state.task_manager(tmgr)

            # The UID will not resolve in the stored PilotManager.
            with pytest.raises(ValueError):
                state.pilot(pilot.uid)

            # The Pilot is detectably invalid.
            with pytest.raises(APIError):
                state.pilot(pilot)
