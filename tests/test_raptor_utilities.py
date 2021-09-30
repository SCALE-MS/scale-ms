import json

import packaging.version
import pytest
import scalems
from scalems.radical.raptor import object_encoder

try:
    import radical.pilot as rp
except ImportError:
    rp = None
else:
    from scalems.radical.raptor import RaptorWorkerConfig
    from scalems.radical.raptor import RaptorWorkerTaskDescription
    from scalems.radical.raptor import worker_script
    from scalems.radical.raptor import check_module_version

pytestmark = pytest.mark.skipif(condition=rp is None,
                                reason='These tests require RADICAL Pilot.')


def test_check_module_version():
    assert not check_module_version('scalems', '1')
    assert check_module_version('scalems', '0a0')


def test_master():
    """Test the details needed to launch the master script.

    We can't actually create a raptor.Master easily, but we can check its bits and pieces.
    """
    worker_description = RaptorWorkerTaskDescription(from_dict={
                # TODO: Don't hard-code this!
                'uid': 'raptor.worker',
                'executable': worker_script(),
                'arguments': [],
                'pre_exec': []
            })
    num_workers = 1
    cores_per_worker = 1
    gpus_per_worker = 0

    client_scalems_version = packaging.version.Version(scalems.__version__)
    if client_scalems_version.is_prerelease:
        minimum_scalems_version = client_scalems_version.public
    else:
        minimum_scalems_version = client_scalems_version.base_version

    # TODO: Add additional dependencies that we can infer from the workflow.
    versioned_modules = (
        ('scalems', minimum_scalems_version),
        ('radical.pilot', rp.version_short)
    )

    configuration = scalems.radical.raptor.Configuration(
        worker=RaptorWorkerConfig(
            descr=worker_description,
            count=num_workers,
            cores=cores_per_worker,
            gpus=gpus_per_worker
        ),
        versioned_modules=list(versioned_modules)
    )
    encoded = json.dumps(configuration, default=object_encoder, indent=2)
    configuration = scalems.radical.raptor.Configuration.from_dict(
        json.loads(encoded)
    )
    for module, version in configuration.versioned_modules:
        found_version = check_module_version(module=module, minimum_version=version)
        assert found_version
