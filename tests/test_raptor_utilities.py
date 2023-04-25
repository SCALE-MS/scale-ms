"""Test the scalems machinery for interacting with the RP raptor facets.

For remote dispatching through RP and Raptor, it can be especially hard to get
good test coverage with any granularity. Here, we do our best to unit test the
important pieces for raptor interaction locally. Full raptor sessions are tested
through :file:`test_rp_exec.py`.
"""

import dataclasses
import json
import logging
import os

import packaging.version
import pytest

import scalems
from scalems.radical.raptor import ClientWorkerRequirements
from scalems.radical.raptor import MasterTaskConfiguration
from scalems.radical.raptor import ScaleMSMaster
from scalems.radical.raptor import ScaleMSWorker
from scalems.radical.raptor import WorkerDescription

try:
    import radical.pilot as rp
except ImportError:
    rp = None
else:
    from scalems.radical.raptor import object_encoder

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pytestmark = pytest.mark.skipif(condition=rp is None, reason="These tests require RADICAL Pilot.")

client_scalems_version = packaging.version.Version(scalems.__version__)
if client_scalems_version.is_prerelease:
    minimum_scalems_version = client_scalems_version.public
else:
    minimum_scalems_version = client_scalems_version.base_version


@pytest.mark.experimental
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
    versioned_modules = (("scalems", minimum_scalems_version), ("radical.pilot", rp.version_short))

    # Note that the Worker launch has unspecified results if the `named_env`
    # does not exist and is not scheduled to be created with `prepare_env`.
    # However, we are not currently using `named_env`. See #90.
    configuration = MasterTaskConfiguration(
        worker=ClientWorkerRequirements(
            named_env="scalems_test_ve", cpu_processes=worker_processes, gpus_per_process=gpus_per_process
        ),
        versioned_modules=list(versioned_modules),
    )
    # Note: *named_env* is unused, pending work on #90 and others.

    conf_dict: scalems.radical.raptor._MasterTaskConfigurationDict = dataclasses.asdict(configuration)
    configuration = MasterTaskConfiguration.from_dict(conf_dict)
    assert configuration.versioned_modules == list(versioned_modules)
    assert configuration.worker.named_env == "scalems_test_ve"

    encoded = json.dumps(configuration, default=object_encoder, indent=2)
    configuration = scalems.radical.raptor.MasterTaskConfiguration.from_dict(json.loads(encoded))
    assert configuration.versioned_modules == [list(module_spec) for module_spec in versioned_modules]
    assert configuration.worker.named_env == "scalems_test_ve"

    with pytest.warns(match="raptor.Master base class"):
        master = ScaleMSMaster(configuration)
    with master.configure_worker(configuration.worker) as worker_configs:
        assert len(worker_configs) == num_workers
        for descr in worker_configs:
            descr: WorkerDescription
            assert descr.ranks == worker_processes
            assert descr.raptor_class == ScaleMSWorker.__name__
            assert os.path.exists(descr.raptor_file)
    assert not os.path.exists(descr.raptor_file)
