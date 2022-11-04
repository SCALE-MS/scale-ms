"""Test the scalems machinery for interacting with the RP raptor facets.

For remote dispatching through RP and Raptor, it can be especially hard to get
good test coverage with any granularity. Here, we do our best to unit test the
important pieces for raptor interaction.
"""

import asyncio
import dataclasses
import json
import os
import warnings

import packaging.version
import pytest

import scalems
from scalems.radical.raptor import ClientWorkerRequirements
from scalems.radical.raptor import MasterTaskConfiguration
from scalems.radical.raptor import ScaleMSMaster
from scalems.radical.raptor import ScaleMSWorker
from scalems.radical.raptor import WorkerDescriptionDict
from scalems.radical.runtime import Runtime

try:
    import radical.pilot as rp
except ImportError:
    rp = None
else:
    from scalems.radical.raptor import RaptorWorkerConfig
    from scalems.radical.raptor import worker_description
    from scalems.radical.raptor import object_encoder

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pytestmark = pytest.mark.skipif(condition=rp is None,
                                reason='These tests require RADICAL Pilot.')

client_scalems_version = packaging.version.Version(scalems.__version__)
if client_scalems_version.is_prerelease:
    minimum_scalems_version = client_scalems_version.public
else:
    minimum_scalems_version = client_scalems_version.base_version


def test_master_configuration_details(rp_venv):
    """Test the details needed to launch the master script.

    WARNING: This test is incomplete. We can't actually create a raptor.Master easily,
    but we can check its bits and pieces. This test mostly checks function signatures
    and data structures. It does not produce a functioning Raptor configuration, or
    even an actual RP Session!
    """

    num_workers = 1
    worker_processes = 1
    gpus_per_process = 0

    # TODO: Add additional dependencies that we can infer from the workflow.
    versioned_modules = (
        ('scalems', minimum_scalems_version),
        ('radical.pilot', rp.version_short)
    )

    configuration = MasterTaskConfiguration(
        worker=ClientWorkerRequirements(
            named_env='scalems_test_ve',
            pre_exec=[],
            cpu_processes=worker_processes,
            gpus_per_process=gpus_per_process
        ),
        versioned_modules=list(versioned_modules)
    )
    # Note: *named_env* is unused, pending work on #90 and others.

    conf_dict: scalems.radical.raptor._MasterTaskConfigurationDict = dataclasses.asdict(configuration)
    configuration = MasterTaskConfiguration.from_dict(conf_dict)
    assert configuration.versioned_modules == list(versioned_modules)
    assert configuration.worker.named_env == 'scalems_test_ve'

    encoded = json.dumps(configuration, default=object_encoder, indent=2)
    configuration = scalems.radical.raptor.MasterTaskConfiguration.from_dict(
        json.loads(encoded)
    )
    assert configuration.versioned_modules == [list(module_spec) for module_spec in versioned_modules]
    assert configuration.worker.named_env == 'scalems_test_ve'

    with pytest.warns(match='raptor.Master base class'):
        master = ScaleMSMaster(configuration)
    with master.configure_worker(configuration.worker) as worker_config:
        assert worker_config['count'] == num_workers
        descr: WorkerDescriptionDict = worker_config['descr']
        assert descr['ranks'] == worker_processes
        assert descr['worker_class'] == ScaleMSWorker.__name__
        assert os.path.exists(descr['worker_file'])
    assert not os.path.exists(descr['worker_file'])


# We either need to mock a RP agent or gather coverage files from remote environments.
# This test does not add any coverage, and only adds measurable coverage or coverage
# granularity if we either mock a RP agent or gather coverage files from remote environments.
@pytest.mark.skip(reason='Incomplete.')
def test_master(rp_task_manager, pilot_description):
    """Check our ability to launch a Master and Worker."""

    session: rp.Session = rp_task_manager.session
    state = Runtime(session=session)
    state.task_manager(rp_task_manager)

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.task_manager')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.db.database')
        warnings.filterwarnings('ignore', category=DeprecationWarning,
                                module='radical.pilot.session')

        # We only expect one pilot
        pilot_dict: dict = rp_task_manager.get_pilots()[0]
        pilot_uid = pilot_dict['uid']
        pmgr_uid = pilot_dict['pmgr']

        pmgr: rp.PilotManager = session.get_pilot_managers(pmgr_uids=pmgr_uid)
        state.pilot_manager(pmgr)

        pilot: rp.Pilot = pmgr.get_pilots(uids=pilot_uid)
        state.pilot(pilot)

        # # Note that the Worker launch has unspecified results if the `named_env`
        # # does not exist and is not scheduled to be created with `prepare_env`.
        # # However, we are not currently using `named_env`. See #90.
        # _worker_description = worker_description(
        #     pre_exec=[],
        #     named_env='scalems_test_ve',
        #     cpu_processes=worker_processes,
        #     gpus_per_process=gpus_per_process
        # )
        # # _worker_description['uid'] = 'raptor-worker-test'
        #
        # configuration: MasterTaskConfiguration = master_input()


@pytest.mark.skip(reason='Temporarily disabled. Needs updates wrt #248, #108.')
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
@pytest.mark.asyncio
async def test_worker(pilot_description, rp_venv, cleandir):
    """Launch the master script and execute a trivial workflow.
    """

    if rp_venv is None:
        # Be sure to provision the venv.
        # pytest.skip('This test requires a user-provided static RP venv.')
        ...

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.getLogger("asyncio").setLevel(logging.DEBUG)

    params = scalems.radical.runtime.Configuration(
        execution_target=pilot_description.resource,
        target_venv=rp_venv,
        rp_resource_params={
            'PilotDescription': pilot_description.as_dict()
        }
    )

    manager = scalems.radical.workflow_manager(loop)
    with scalems.workflow.scope(manager):
        assert not loop.is_closed()
        # Enter the async context manager for the default dispatcher
        async with manager.dispatch(params=params) as dispatcher:
            # We have now been through RPDispatchingExecutor.runtime_startup().
            assert isinstance(dispatcher, scalems.radical.runtime.RPDispatchingExecutor)
            logger.debug(f'Session is {repr(dispatcher.runtime.session)}')

            scheduler = dispatcher.runtime.scheduler.uid
            task_uid = 'task.scalems-test-worker'
            # TODO: Encode work in task args/kwargs.
            args = []
            kwargs = {}
            task_description = rp.TaskDescription(
                {
                    'scheduler': scheduler,
                    'uid': task_uid,
                    'mode': rp.TASK_FUNCTION,
                    'cpu_processes': 1,
                    'cpu_process_type': rp.SERIAL,
                    'function': scalems_task_wrapper(*args, **kwargs),
                    # 'args': [],
                    # 'kwargs': {}
                }
            )

            # Submit a raptor task
            # task = dispatcher.runtime.task_manager().submit_tasks(task_description)
            # TODO: Implement scalems.radical.raptor.dispatch()

            # task.wait()
            # exc_typename, exc_message = task.exception
            # assert exc_typename.endswith('MissingImplementationError')

            # TODO: Flesh out the Master result_cb().
            # TODO: Check an actual data result.
