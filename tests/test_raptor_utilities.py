import json

import packaging.version
import pytest
import scalems

try:
    import radical.pilot as rp
except ImportError:
    rp = None
else:
    from scalems.radical.common import RaptorWorkerConfig
    from scalems.radical.raptor import check_module_version
    from scalems.radical.common import worker_description
    from scalems.radical.raptor import object_encoder

pytestmark = pytest.mark.skipif(condition=rp is None,
                                reason='These tests require RADICAL Pilot.')


def test_check_module_version():
    assert not check_module_version('scalems', '1')
    assert check_module_version('scalems', '0a0')


def test_master():
    """Test the details needed to launch the master script.

    We can't actually create a raptor.Master easily, but we can check its bits and pieces.
    """

    num_workers = 1
    cores_per_worker = 1
    gpus_per_worker = 0
    _worker_description = worker_description(
        named_env='scalems_test_ve',
        cpu_processes=cores_per_worker,
        gpu_processes=gpus_per_worker,
        worker_class=None,
        worker_file=None,
    )
    _worker_description['uid'] = 'raptor-worker-test'

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
            descr=_worker_description,
            count=num_workers
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
