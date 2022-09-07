"""Tests related to RP handling of virtual environments."""
import logging
import os
import shlex
import shutil
import subprocess
import typing
from urllib.parse import ParseResult
from urllib.parse import urlparse

import packaging.version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_prepare_venv(rp_task_manager, sdist, rp_venv):
    """Bootstrap the scalems package in a RP target environment using pilot.prepare_env.

    Note that we cannot wait on the environment preparation directly, but we can define
    a task with ``named_env`` matching the *prepare_env* key to implicitly depend on
    successful creation.
    """

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
        'ru': ru.sdist_name,
        'rs': rs.sdist_name,
        'rp': rp.sdist_name,
        'scalems': os.path.basename(sdist),
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

    sandbox_path = urlparse(pilot.pilot_sandbox).path

    sdist_session_paths = {name: os.path.join(sandbox_path, sdist_names[name]) for name in sdist_names.keys()}

    logger.debug('Staging ' + ', '.join(sdist_session_paths.values()))

    input_staging = []
    for name in sdist_names.keys():
        input_staging.append({
            'source': sdist_local_paths[name],
            'target': sdist_names[name],
            'action': rp.TRANSFER
        })
    logger.debug(str(input_staging))
    pilot.stage_in(input_staging)

    tmgr = rp_task_manager

    packages = [
        'pip',
        'setuptools',
        'wheel']
    packages.extend(sdist_session_paths.values())

    # We test in multiple environments, so we have to check what Python
    # interpreter is installed in the current target resource.
    # Note that, at this time, we use a single (user-specified) venv for the
    # Pilot agent and for the tasks.
    executable = os.path.join(rp_venv, 'bin', 'python3')
    scriptlet = 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
    access = pilot.description['access_schema']
    if access == 'local':
        command = [
            executable,
            '-c',
            scriptlet]
        process = subprocess.run(
            args=command,
            capture_output=True,
            check=True,
            text=True
        )
    else:
        # We don't have automated handling for other access methods at this time.
        assert access == 'ssh'

        domain, target = str(pilot.resource).split('.')
        resource_config = ru.Config(
            module='radical.pilot.resource',
            name=domain)[target]
        ssh_target = resource_config[access]['job_manager_endpoint']
        result: ParseResult = urlparse(ssh_target)
        assert result.scheme == 'ssh'
        user = result.username
        port = result.port
        host = result.hostname

        ssh = [shutil.which('ssh')]
        if user:
            ssh.extend(['-l', user])
        if port:
            ssh.extend(['-p', str(port)])
        ssh.append(host)

        command = [
            executable,
            '-c',
            shlex.quote(scriptlet)]

        logger.debug(f'Executing subprocess {ssh + command}')
        process = subprocess.run(
            ssh + command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            encoding='utf-8')
        if process.returncode != 0:
            logger.error('Failed ssh stdout: ' + str(process.stdout))
            logger.error('Failed ssh stderr: ' + str(process.stderr))

    assert process.returncode == 0
    python_version = process.stdout.rstrip()
    logger.debug(f'Requesting Python version {python_version}.')
    pilot.prepare_env(env_name='scalems_env',
                      env_spec={'type': 'virtualenv',
                                'version': python_version,
                                'setup': packages})

    rp_check_desc = rp.TaskDescription(
        {
            'executable': 'python3',
            'arguments': [
                '-c',
                'import radical.pilot as rp; print(rp.version_detail)'
            ],
            'named_env': 'scalems_env'
        }
    )
    scalems_check_desc = rp.TaskDescription(
        {
            'executable': 'python3',
            'arguments': [
                '-c',
                'import scalems; print(scalems.__file__)'
            ],
            'named_env': 'scalems_env'
        }
    )

    rp_check_task, scalems_check_task = \
        tmgr.submit_tasks([rp_check_desc, scalems_check_desc])
    tmgr.wait_tasks()

    rp_version = rp_check_task.stdout.rstrip()
    rp_version = packaging.version.parse(rp_version)
    if rp_check_task.stdout:
        logger.debug(f'RP version details: {rp_version}')
    if rp_check_task.stderr:
        logger.debug(f'Task stderr: {rp_check_task.stderr}')
    assert rp_version == packaging.version.parse(rp.version)
    assert rp_check_task.exit_code == 0

    if scalems_check_task.stdout:
        logger.debug(f'scalems package module: {scalems_check_task.stdout}')
    if scalems_check_task.stderr:
        logger.debug(f'Task stderr: {scalems_check_task.stderr}')
    assert scalems_check_task.exit_code == 0

    # Check for fix to https://github.com/radical-cybertools/radical.pilot/issues/2624
    if rp_version >= packaging.version.parse('1.15'):
        td = rp.TaskDescription(
            {
                'executable': 'python3',
                'arguments': ['-c', 'import sys; print(sys.version)'],
                'named_env': 'scalems_env'
            }
        )
        task = tmgr.submit_tasks(td)
        tmgr.wait_tasks()
        assert task.exit_code == 0
        requested_version = packaging.version.parse(python_version)
        remote_py_version = '.'.join(task.stdout.rstrip().split('.')[0:2])
        remote_py_version = packaging.version.parse(remote_py_version)
        assert requested_version == remote_py_version
