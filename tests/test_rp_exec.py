"""Dispatch scalems.exec through RADICAL Pilot.

In the first draft, we keep the tests simpler by assuming we are invoked in an
environment where RP is already configured. In a follow-up, we will use a
dispatching layer to run meaningful tests through an RP Context specialization.
In turn, the initial RP dispatching will probably use a Docker container to
encapsulate the details of the RP-enabled environment, such as the required
MongoDB instance and RADICAL_PILOT_DBURL environment variable.
"""

import os

import pytest


def get_rp_decorator():
    """Decorator for tests that should be run in a RADICAL Pilot environment only."""
    try:
        import radical.pilot as rp
        import radical.utils as ru
    except ImportError:
        rp = None
        ru = None

    with_radical_only = pytest.mark.skipif(
        rp is None or ru is None or 'RADICAL_PILOT_DBURL' not in os.environ,
        reason="Test requires RADICAL environment.")

    # The above logic may not be sufficient to mark the usability of the RP environment.
    if rp is not None and 'RADICAL_PILOT_DBURL' in os.environ:
        try:
            with rp.Session():
                with_radical_only = pytest.mark.RADICAL_PILOT
        except:
            with_radical_only = pytest.mark.skip(reason="Cannot create radical.pilot.Session")

    return with_radical_only


# Decorator for tests that should be run in a RADICAL Pilot environment only.
with_radical_only = get_rp_decorator()


@pytest.fixture
def rp_config():
    """Provide a RADICAL Pilot Resource Config to a test suite.

    The 'resource' key in a Pilot Description must name a key that the Session
    can use to get default values for the execution environment.
    """
    # Ref: https://radicalpilot.readthedocs.io/en/stable/machconf.html#customizing-resource-configurations-programatically
    import radical.pilot as rp
    import radical.utils as ru
    # TODO: Resolve usage error.
    # Ref: https://github.com/radical-cybertools/radical.pilot/issues/2181
    try:
        cfg = rp.ResourceConfig('local.localhost', ru.Config('radical.pilot.session', name='default', cfg=None))
    except:
        cfg = dict()
    # `local.localhost` is preconfigured, but some of the properties are likely not appropriate.
    # Ref: https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_local.json
    # TODO: Is there a more canonical way to programmatically generate a valid config?
    # Ref: https://radicalpilot.readthedocs.io/en/stable/machconf.html#writing-a-custom-resource-configuration-file
    # TODO: Set a sensible number of cores / threads / GPUs.
    return dict(config=cfg, rp=rp, ru=ru)


@with_radical_only
def test_rp_import():
    """Confirm availability of RADICAL Pilot infrastructure.

    Tests here may be too cumbersome to run in every invocation of a pytest fixture,
    so let's just run them once in this unit test.
    """
    import radical.pilot as rp
    import radical.utils as ru
    assert rp is not None
    assert ru is not None
    assert 'RADICAL_PILOT_DBURL' in os.environ
    # TODO: Assert the presence of required ResourceConfig source file(s)...
    # assert os.path.exists()


# Note: radical.pilot.Session creation causes several deprecation warnings.
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@with_radical_only
def test_rp_basic_task(rp_config):
    rp = rp_config['rp']

    # Note: Session creation will fail with a FileNotFound error unless venv is explicitly `activate`d.
    # TODO: Figure out what `activate` does that `rp-venv/bin/python` doesn't do.
    with rp.Session() as session:
        # Based on `radical.pilot/examples/config.json`
        # TODO: Does the Session have a default spec for 'local.localhost'? Can/should we reference it?
        # See also https://github.com/radical-cybertools/radical.pilot/issues/2181
        resource = 'local.localhost'
        resource_config = {resource: {}}
        if resource in rp_config['config']:
            resource_config[resource].update(rp_config.config[resource])
        resource_config[resource].update({
            'project': None,
            'queue': None,
            'schema': None,
            'cores': 1,
            'gpus': 0
        })

        pilot_description = dict(resource=resource,
                                 runtime=30,
                                 exit_on_error=True,
                                 project=resource_config[resource]['project'],
                                 queue=resource_config[resource]['queue'],
                                 cores=resource_config[resource]['cores'],
                                 gpus=resource_config[resource]['gpus'])

        task_description = {'executable': '/bin/date',
                            'cpu_processes': 1,
                            }

        pmgr = rp.PilotManager(session=session)
        umgr = rp.UnitManager(session=session)
        pilot = pmgr.submit_pilots(rp.ComputePilotDescription(pilot_description))
        task = umgr.submit_units(rp.ComputeUnitDescription(task_description))

        umgr.add_pilots(pilot)
        umgr.wait_units()

        assert task.exit_code == 0
