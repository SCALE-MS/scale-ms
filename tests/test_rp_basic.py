"""Test basic RADICAL Pilot usability.

Note: `export RADICAL_LOG_LVL=DEBUG` to enable RP debugging output.
"""
import logging
import subprocess

import pytest

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
            or 'local' in pilot_description.resource \
            or pilot_description.resource == 'docker.login':
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
