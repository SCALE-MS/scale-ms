"""Tests related to RP handling of virtual environments."""
import logging
import os
import typing
import urllib.parse

import pytest

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.skip(reason='Unimplemented.')
def test_rp_static_venv(rp_venv, pilot_description):
    """Confirm that a prepared venv is usable by RP.

    .. todo:: Are we using this for the Pilot? Or just the Tasks?
    """
    # If scalems is installed and usable, and the venv is activated,
    # then the `scalems_rp_master` entry point script should be discoverable with `which`
    # and executable.
    ...


def test_prepare_venv(rp_task_manager, sdist):
    """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.

    This test function specifically tests the local.localhost resource.

    Note that we cannot wait on the environment preparation directly, but we can define
    a task with ``named_env`` matching the *prepare_env* key to implicitly depend on
    successful creation.
    """
    # TODO: Merge with test_rp_raptor_local but use the installed scalems_rp_master and
    #  scalems_rp_worker scripts.

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
    logger.debug(f'Task stderr: {task.stderr}')
    assert task.exit_code == 0