#!/usr/bin/env python3

"""Dispatch scalems.exec through RADICAL Pilot.

In the first draft, we keep the tests simpler by assuming we are invoked in an
environment where RP is already configured. In a follow-up, we will use a
dispatching layer to run meaningful tests through an RP Context specialization.
In turn, the initial RP dispatching will probably use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.
"""
import asyncio
import logging
import os
import json
import warnings

import pytest
import scalems
import scalems.context
import scalems.radical


def get_rp_decorator():
    '''
    Decorator for tests that should be run in a RADICAL Pilot environment only.
    '''

    try:
        import radical.pilot as rp
        import radical.utils as ru

    except ImportError:
        rp = None
        ru = None

    with_radical_only = pytest.mark.skipif(
        rp is None or
        ru is None or
        not os.environ.get('RADICAL_PILOT_DBURL'),
        reason="Test requires RADICAL environment.")

    # NOTE: session creation is a non-trivial operation in RP and
    #       should not be part of a decorator or test, IMHO. if the above is
    #       insufficient, then the test should rightly fail. (AM)

    return with_radical_only


# Decorator for tests that should be run in a RADICAL Pilot environment only.
with_radical_only = get_rp_decorator()


@with_radical_only
def test_rp_usability():
    """Confirm availability of RADICAL Pilot infrastructure.

    Tests here may be too cumbersome to run in every invocation of a
    pytest fixture, so let's just run them once in this unit test.
    """

    import radical.pilot as rp

    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=DeprecationWarning)
        with rp.Session():
            ...


# ------------------------------------------------------------------------------
#
# Note: radical.pilot.Session creation causes several deprecation warnings.
# Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
#
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@with_radical_only
def test_rp_basic_task():

    import radical.pilot as rp

    # Note: Session creation will fail with a FileNotFound error unless venv
    #       is explicitly `activate`d (or the scripts installed with RADICAL components
    #       are otherwise made available on the PATH).
    with rp.Session() as session:

        # Based on `radical.pilot/examples/config.json`
        # TODO: Does the Session have a default spec for 'local.localhost'?
        #       Can/should we reference it?
        #       https://github.com/radical-cybertools/radical.pilot/issues/2181
        # NOTE: a session does not have a spec, really - the resource config
        #       should be a *static* description of the target resource and
        #       should not need any changing. (AM)
        pd = rp.PilotDescription({'resource': 'local.localhost',
                                  'cores'   : 32,
                                  'gpus'    : 1})

        td = rp.TaskDescription({'executable'   : '/bin/date',
                                 'cpu_processes': 1})

        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)

        pilot = pmgr.submit_pilots(pd)
        task  = tmgr.submit_tasks(td)

        tmgr.add_pilots(pilot)
        tmgr.wait_tasks(uids=[task.uid])

        assert task.exit_code == 0

    assert session.closed


# ------------------------------------------------------------------------------
#
# Note: radical.pilot.Session creation causes several deprecation warnings.
# Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
#
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@with_radical_only
def test_rp_scalems():

    import radical.pilot as rp

    with rp.Session() as session:

        # define a pilot and launch it
        pd    = rp.PilotDescription(
                   {'resource': 'local.localhost',
                    'cores'   : 4,
                    'gpus'    : 0})

        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)
        pilot = pmgr.submit_pilots(pd)
        tmgr.add_pilots(pilot)

        # TODO: How can we recover successful workflow stages from previous failed Sessions?
        #
        # The client needs to note the sandbox locations from runs. SCALEMS can
        # then manage / maintain state tracking or state discovery to optimize workflow recovery.
        # Resumed workflows can make reference to sandboxes from previous sessions
        # (RCT work in progress: https://github.com/radical-cybertools/radical.pilot/tree/feature/sandboxes
        # slated for merge in 2021 Q2 to support `sandbox://` URIs).

        # define a raptor.scalems master and launch it within the pilot
        pwd   = os.path.dirname(__file__)
        td    = rp.TaskDescription(
                {
                    'uid'          :  'raptor.scalems',
                    'executable'   :  'python3',
                    'arguments'    : ['./scalems_test_master.py', '%s/scalems_test_cfg.json'  % pwd],
                    'input_staging': ['%s/scalems_test_cfg.json'  % pwd,
                                      '%s/scalems_test_master.py' % pwd,
                                      '%s/scalems_test_worker.py' % pwd]
                })
        scheduler = tmgr.submit_tasks(td)

        # define raptor.scalems tasks and submit them to the master
        tds = list()
        for i in range(2):
            uid  = 'scalems.%06d' % i
            # ------------------------------------------------------------------
            # work serialization goes here
            # This dictionary is interpreted by rp.raptor.Master.
            work = json.dumps({'mode'      :  'call',
                               'cores'     :  1,
                               'timeout'   :  10,
                               'data'      : {'method': 'hello',
                                              'kwargs': {'world': uid}}})
            # ------------------------------------------------------------------
            tds.append(rp.TaskDescription({
                               'uid'       : uid,
                               'executable': 'scalems',  # This field is ignored by the ScaleMSMaster that receives this submission.
                               'scheduler' : 'raptor.scalems', # 'scheduler' references the task implemented as a
                               'arguments' : [work]  # Processed by raptor.Master._receive_tasks
            }))

        tasks = tmgr.submit_tasks(tds)
        assert len(tasks) == len(tds)
        # 'arguments' gets wrapped in a Request at the Master by _receive, then
        # picked up by the Worker in _request_cb. Then picked up in forked interpreter
        # by Worker._dispatch, which checks the *mode* of the Request and dispatches
        # according to native or registered mode implementations. (e.g. 'call' (native) or 'scalems')

        # task process is launched with Python multiprocessing (native) module and added to self._pool.
        # When the task runs, it's result triggers _result_cb

        # wait for *those* tasks to complete and report results
        tmgr.wait_tasks(uids=[t.uid for t in tasks])

        # Cancel the master.
        tmgr.cancel_tasks(uids=scheduler.uid)
        # Cancel blocks until the task is done so the following wait it currently redundant,
        # but there is a ticket open to change this behavior.
        # See https://github.com/radical-cybertools/radical.pilot/issues/2336
        # tmgr.wait_tasks([scheduler.uid])

        for t in tasks:
            print('%s  %-10s : %s' % (t.uid, t.state, t.stdout))
            assert t.state == rp.states.DONE
            assert t.exit_code == 0


    assert session.closed


# ------------------------------------------------------------------------------
#
# Note: radical.pilot.Session creation causes several deprecation warnings.
# Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
#
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@pytest.mark.asyncio
@with_radical_only
async def test_exec_rp():
    original_context = scalems.context.get_context()
    asyncio.get_event_loop().set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)
    # Test RPDispatcher context
    context = scalems.radical.RPWorkflowContext()
    with scalems.context.scope(context):
        ...
        # TODO: re-enable test for basic executable wrapper.
        # async with context.dispatch():
        #     cmd = scalems.executable(('/bin/echo',))


    # Test active context scoping.
    assert scalems.context.get_context() is original_context
