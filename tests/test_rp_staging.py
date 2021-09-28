#!/usr/bin/env python3

"""Test data staging.

Note: `export RADICAL_LOG_LVL=DEBUG` to enable RP debugging output.
"""
import json
import logging
import os
import warnings

import pytest

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# TODO: Catch sigint from RP and apply our own timeout.


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
    # if pilot_description.access_schema \
    #         and pilot_description.access_schema != 'local':
    #     pytest.skip('This test is only for local execution.')

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
        pilot_description.exit_on_error = False
        pilot_description.input_staging = [fpath]
        with open(fpath, 'w') as fh:
            fh.writelines([data])
        try:
            pilot = pmgr.submit_pilots(pilot_description)
            # Confirmation that the input file has been staged by waiting for pilot state.
            pilot.wait(state=[rp.states.PMGR_ACTIVE] + rp.FINAL, timeout=240)
        finally:
            os.unlink(fpath)
        assert pilot.state not in rp.FINAL

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

        logger.debug('Submitting master task.')
        master = tmgr.submit_tasks(td)

        # Illustrate availability of scheduler and of data staged with Master task.
        # When the task enters AGENT_SCHEDULING_PENDING it has passed all input staging,
        # and the files will be available.
        # (see https://docs.google.com/drawings/d/1q5ehxIVdln5tXEn34mJyWAmxBk_DqZ5wwkl3En-t5jo/)

        # Confirm that Master script is running (and ready to receive raptor tasks)
        # WARNING: rp.Task.wait() *state* parameter does not handle tuples,
        #  but does not check type.
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
        logger.debug('submitting raptor tasks.')
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
        logger.debug('waiting for results')
        states = tmgr.wait_tasks(uids=[t.uid for t in tasks], timeout=240)
        logger.debug(f'Task states: {" ".join([str(state) for state in states])}')
        # Tasks are expected to complete or fail in a reasonable amount of time.
        assert all([state in rp.FINAL for state in states])

        # Cancel the master.
        logger.debug('canceling master task')
        tmgr.cancel_tasks(uids=master.uid)
        # Cancel blocks until the task is done so the following wait it currently redundant,
        # but there is a ticket open to change this behavior.
        # See https://github.com/radical-cybertools/radical.pilot/issues/2336
        logger.debug('canceling any remaining tasks')
        states = tmgr.wait_tasks(timeout=120)
        logger.debug(f'Task states: {" ".join([str(state) for state in states])}')

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

        logger.debug('Canceling pilot.')
        pilot.cancel()
        logger.debug('Closing task manager.')
        tmgr.close()
        logger.debug('Closing pilot manager.')
        pmgr.close()

    finally:
        logger.debug('Closing session.')
        session.close(download=False)


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
    test_rp_raptor_staging(pdesc, venv)
