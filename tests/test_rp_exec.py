#!/usr/bin/env python3

"""Dispatch scalems.exec through RADICAL Pilot.

In the first draft, we keep the tests simpler by assuming we are invoked in an
environment where RP is already configured. In a follow-up, we will use a
dispatching layer to run meaningful tests through an RP Context specialization.
In turn, the initial RP dispatching will probably use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.
"""

import os
import time
import json
import warnings

import pytest
from radical.pilot.states import AGENT_EXECUTING
import scalems
import scalems.context
import scalems.radical


# ------------------------------------------------------------------------------
#
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

    with_radical_only = pytest.mark.skipif(False,
                                           reason="RP should be available.")

    # The above logic may not be sufficient to mark the usability
    # of the RP environment.
  # if rp is not None and not os.environ('RADICAL_PILOT_DBURL'):
  #     try:
  #         # Note: radical.pilot.Session creation causes several
  #         #       deprecation warnings.
  #         # Ref:  https://github.com/radical-cybertools/radical.pilot/\
  #         #       issues/2185
  #         with warnings.catch_warnings():
  #             warnings.simplefilter('ignore', category=DeprecationWarning)
  #             with rp.Session():
  #                 with_radical_only = pytest.mark.skipif(False,
  #                                            reason="RP should be available.")
  #     except:
  #         with_radical_only = pytest.mark.skip(reason="Can't load rp.Session")

    return with_radical_only


# Decorator for tests that should be run in a RADICAL Pilot environment only.
with_radical_only = get_rp_decorator()


# ------------------------------------------------------------------------------
#
# NOTE: a RP resource config is *not* a pilot config - it tells the pilot how
#      a resource is configured.  I don't think this code needs to create or
#      load a resource config at all. (AM)
#
# @pytest.fixture
# def rp_config():
#     """Provide a RADICAL Pilot Resource Config to a test suite.
#
#     The 'resource' key in a Pilot Description must name a key that the Session
#     can use to get default values for the execution environment.
#     """
#     # Ref: https://radicalpilot.readthedocs.io/en/stable/ \
#     #      machconf.html#customizing-resource-configurations-programatically
#     import radical.pilot as rp
#     import radical.utils as ru
#     # TODO: Resolve usage error.
#     # Ref: https://github.com/radical-cybertools/radical.pilot/issues/2181
#     try:
#         cfg = rp.ResourceConfig('local.localhost',
#                  ru.Config('radical.pilot.session', name='default', cfg=None))
#     except:
#         cfg = dict()
#
#     # `local.localhost` is preconfigured, but some of the properties are
#     # likely not appropriate.
#     # NOTE: localhost is not an interesting resource though.  In general,
#     #       resource configs are *static* representations of resource
#     #       configurations and don't need changing.
#     # Ref: https://github.com/radical-cybertools/radical.pilot/blob/ \
#     #         devel/src/radical/pilot/configs/resource_local.json
#     #
#     # TODO: Is there a more canonical way to programmatically generate a
#     #       valid config?
#     # Ref:  https://radicalpilot.readthedocs.io/en/stable/machconf.html\
#     #              #writing-a-custom-resource-configuration-file
#     # TODO: Set a sensible number of cores / threads / GPUs.
#     return dict(config=cfg, rp=rp, ru=ru)


# ------------------------------------------------------------------------------
#
@with_radical_only
def test_rp_import():
    """Confirm availability of RADICAL Pilot infrastructure.

    Tests here may be too cumbersome to run in every invocation of a
    pytest fixture, so let's just run them once in this unit test.
    """

    # FIXME: this test is not useful if `if_radical_only` applies - that
    #        decorator runs the very same tests already.

    import radical.pilot as rp
    import radical.utils as ru

    assert rp is not None
    assert ru is not None
    assert os.environ.get('RADICAL_PILOT_DBURL')

    # TODO: Assert the presence of required ResourceConfig source file(s)...
    #       assert os.path.exists()
    # NOTE: resource config files are RP *internal* config files and should not
    #       be asserted - location and names may change.


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
    #       is explicitly `activate`d.
    # TODO: what does `activate` do that `rp-venv/bin/python` doesn't do?
    # NOTE: an RP session does not need 'activation' - or I misunderstand
    #       what the `activate` refers to? (AM)
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
        task  = tmgr.submit_units(td)

        tmgr.add_pilots(pilot)
        tmgr.wait_units()

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
                    'cores'   : 32,
                    'gpus'    : 1})

        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)
        pilot = pmgr.submit_pilots(pd)
        tmgr.add_pilots(pilot)

        # define a raptor.scalems master and launch it within the pilot
        pwd   = os.path.dirname(__file__)
        td    = rp.TaskDescription(
                {
                    'uid'          :  'raptor.scalems',
                    'executable'   :  '%s/scalems_test_master.py' % pwd,
                    'arguments'    : ['%s/scalems_test_cfg.json'  % pwd],
                    'input_staging': ['%s/scalems_test_cfg.json'  % pwd,
                                      '%s/scalems_test_master.py' % pwd,
                                      '%s/scalems_test_worker.py' % pwd]
                })
        scheduler = tmgr.submit_tasks(td)

        # FIXME
        # tmgr.wait_tasks(uids=master.uid, state=rp.AGENT_EXECUTING)

        # define raptor.scalems tasks and submit them to the master
        tds = list()
        for i in range(10):
            uid  = 'scalems.%06d' % i
            # ------------------------------------------------------------------
            # work serialization goes here
            work = json.dumps({'mode'      :  'call',
                               'cores'     :  1,
                               'timeout'   :  100,
                               'data'      : {'method': 'hello',
                                              'kwargs': {'world': uid}}})
            # ------------------------------------------------------------------
            tds.append(rp.TaskDescription({
                               'uid'       : uid,
                               'executable': 'scalems',
                               'scheduler' : 'raptor.scalems',
                               'arguments' : [work]}))

        tasks = tmgr.submit_tasks(tds)

        # wait for *those* tasks to complete and report results
        tmgr.wait_tasks(uids=[t.uid for t in tasks])
        for t in tasks:
            print('%s  %-10s : %s' % (t.uid, t.state, t.stdout))

        tmgr.cancel_tasks(uids=scheduler.uid)
        tmgr.wait_tasks()


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
    # Test RPDispatcher context
    # Note that a coroutine object created from an `async def` function is
    # only awaitable once.
    context = scalems.radical.RPWorkflowContext()
    async with context as session:
        scalems.executable(('/bin/date',))
        await session.run()
    # Test active context scoping.
    assert scalems.context.get_context() is original_context


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

  # import asyncio
  # asyncio.run(test_exec_rp())

    test_rp_scalems()


# ------------------------------------------------------------------------------

