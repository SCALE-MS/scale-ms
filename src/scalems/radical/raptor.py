"""Support for scalems on radical.pilot.raptor.

Client:
    The client should be reasonably certain that the target environment has a
    compatible installation of RP and scalems. A rp.raptor.Master task script and a
    rp.raptor.Worker task script are installed with the scalems package. Their names
    are returned by the module functions ``master_script()`` and ``worker_script()``,
    and they should be resolvable on the PATH for a Python interpreter process in an
    environment that has the scalems package installed.

"""
import argparse
import dataclasses
import functools
import importlib
import importlib.metadata
import json
import os
import sys
import time
import typing
from importlib.machinery import ModuleSpec
from importlib.util import find_spec

import packaging.version

from scalems.radical._common import RequestInput
from scalems.radical._common import RaptorWorkerConfig
from scalems.radical._common import RaptorWorkerConfigDict
from scalems.radical._common import RequestInputList

try:
    import radical.pilot as rp
    from radical.pilot.raptor.request import Request
    # TODO (#100): This would be a good place to add some version checking.
except ImportError:
    raise RuntimeError('RADICAL Pilot installation not found.')
else:
    # We import rp before `logging` to avoid warnings when rp monkey-patches the
    # logging module. This `try` suite helps prevent auto-code-formatters from
    # rearranging the import order of built-in versus third-party modules.
    import logging

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)


class ScaleMSTaskDescription(typing.TypedDict):
    """SCALE-MS specialization of extra raptor task info.

    This structure is serialized into the TaskDescription.arguments[0] of a rp.Task
    submitted with a *scheduler* set to a ``scalems_rp_master`` Task uid.

    When deserialized, the object is treated as the prototype for a _RequestInput.

    The mapping is assumed to contain keys *mode*, *data*, and *timeout*
    when it becomes incorporated into a new Request object (as the core
    data member) after Master.request_cb(). Optionally, it may contain
    *cores* and *gpus*.
    """
    mode: str
    data: typing.Any
    timeout: typing.SupportsFloat
    cores: typing.Optional[int]
    gpus: typing.Optional[int]


class ScaleMSRequestInput(RequestInput, ScaleMSTaskDescription):
    """"""


EncodableAsDict = typing.Mapping[str, 'Encodable']
EncodableAsList = typing.List['Encodable']
Encodable = typing.Union[str, int, float, bool, None, EncodableAsDict, EncodableAsList]


# def object_decoder(obj: dict):
#     """Provide the object_hook callback for a JSONDecoder."""


@functools.singledispatch
def object_encoder(obj) -> Encodable:
    """Provide the *default* callback for JSONEncoder."""
    raise TypeError(f'No decoder for {obj.__class__.__qualname__}.')


# def get_decoder() -> json.JSONDecoder:
#     """Get a JSONDecoder instance extended for types in this module."""
#     decoder = json.JSONDecoder(object_hook=object_decoder)
#     return decoder
#
#
# def get_encoder() -> json.JSONEncoder:
#     """Get a JSONEncoder instance extended for types in this module."""
#     encoder = json.JSONEncoder(default=object_encoder)
#     return encoder


class _ConfigurationDict(typing.TypedDict):
    worker: RaptorWorkerConfigDict
    versioned_modules: typing.List[typing.Tuple[str, str]]


@object_encoder.register
def _(obj: rp.TaskDescription) -> dict:
    return obj.as_dict()


@object_encoder.register
def _(obj: RaptorWorkerConfig) -> dict:
    return dataclasses.asdict(obj)


@dataclasses.dataclass
class Configuration:
    """Input to the script responsible for the RP raptor Master."""
    worker: RaptorWorkerConfig
    versioned_modules: typing.List[typing.Tuple[str, str]]

    @classmethod
    def from_dict(cls: typing.Type['Configuration'],
                  obj: _ConfigurationDict) -> 'Configuration':
        return cls(
            worker=RaptorWorkerConfig.from_dict(obj['worker']),
            versioned_modules=list(obj['versioned_modules'])
        )


@object_encoder.register
def _(obj: Configuration) -> dict:
    return dataclasses.asdict(obj)


@cache
def master_script() -> str:
    """Get the name of the RP raptor master script.

    The script to run a RP Task based on a rp.raptor.Master is installed
    with :py:mod`scalems`. Installation configures an "entry point" script
    named ``scalems_rp_master``, but for generality this function should
    be used to get the entry point name.

    Before returning, this function confirms the availability of the entry point
    script in the current Python environment. A client should arrange for
    the script to be called in the execution environment and to confirm
    that the (potentially remote) entry point matches the expected API.
    """
    try:
        import pkg_resources
    except ImportError:
        pkg_resources = None
    master_script = 'scalems_rp_master'
    if pkg_resources is not None:
        # It is not hugely important if we cannot perform this test.
        # In reality, this should be performed at the execution site, and we can/should
        # remove the check here once we have effective API compatibility checking.
        # See https://github.com/SCALE-MS/scale-ms/issues/100
        assert pkg_resources.get_entry_info('scalems', 'console_scripts',
                                            'scalems_rp_master').name == master_script
    return master_script


@cache
def worker_script() -> str:
    """Get the name of the RP raptor master script.

    The script to run a RP Task based on a rp.raptor.Worker is installed
    with :py:mod`scalems`. Installation configures an "entry point" script
    named ``scalems_rp_worker``, but for generality this function should
    be used.

    Before returning, this function confirms the availability of the entry point
    script in the current Python environment. A client should arrange for
    the script to be called in the execution environment and to confirm
    that the (potentially remote) entry point matches the expected API.
    """
    try:
        import pkg_resources
    except ImportError:
        pkg_resources = None
    worker_script = 'scalems_rp_worker'
    if pkg_resources is not None:
        # It is not hugely important if we cannot perform this test.
        # In reality, this should be performed at the execution site, and we can/should
        # remove the check here once we have effective API compatibility checking.
        # See https://github.com/SCALE-MS/scale-ms/issues/100
        assert pkg_resources.get_entry_info('scalems', 'console_scripts',
                                            'scalems_rp_worker').name == worker_script
    return worker_script


class SoftwareCompatibilityError(RuntimeError):
    """Incompatible package versions or software interfaces."""


def check_module_version(module: str, minimum_version: str):
    """Get version metadata for importable module an check that it is at least version."""
    try:
        found_version = importlib.metadata.version(module)
    except importlib.metadata.PackageNotFoundError:
        spec: ModuleSpec = find_spec(module)
        found_version = importlib.metadata.version(spec.parent)
    found_version = packaging.version.Version(found_version)
    minimum_version = packaging.version.Version(minimum_version)
    if found_version < minimum_version:
        return False
    return found_version


class ScaleMSMaster(rp.raptor.Master):
    def __init__(self, configuration: Configuration):
        for module, version in configuration.versioned_modules:
            logger.debug(f'Looking for {module} version {version}.')
            found_version = check_module_version(module=module, minimum_version=version)
            if found_version:
                logger.debug(f'Found {module} version {found_version}: Okay.')
            else:
                raise SoftwareCompatibilityError(
                    f'{module} version not compatible with {version}.'
                )
        super(ScaleMSMaster, self).__init__()

    def result_cb(self, requests: typing.Sequence[Request]):
        for r in requests:
            logger.info('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))

    def request_cb(self, requests: RequestInputList) -> RequestInputList:
        """Allows all incoming requests to be processed by the Master.

        Request is a dictionary deserialized from Task.description.arguments[0],
        plus three additional keys.

        Note that Tasks may not be processed in the same order in which they are submitted
        by the client.

        If overridden, request_cb() must return a list (presumably, the same as the list
        of the requests that should be processed normally after the callback). This allows
        subclasses of rp.raptor.Master to add or remove requests before they become Tasks.
        The returned list is submitted with self.request() by the base class after the
        callback returns.

        A Master may call self.request() to self-submit items (e.g. instead of or in
        addition to manipulating the returned list in request_cb()).

        It is the developer's responsibility to choose unique task IDs (uid) when crafting
        items for Master.request().
        """
        # for req_input in requests:
        #     request = Request(req=req_input)
        return requests


parser = argparse.ArgumentParser()
parser.add_argument(
    'file',
    type=str,
    help='Input file (JSON) for configuring ScaleMSMaster instance.'
)


def master():
    """Entry point for raptor.Master task.

    This function implements the scalems_rp_master entry point script called by the
    RADICAL Pilot executor to provide the raptor master task.
    """
    if not os.environ['RP_SESSION_ID']:
        raise RuntimeError('Raptor Master must be launched by RP executor.')

    args = parser.parse_args()
    if not os.path.exists(args.file):
        raise RuntimeError(f'File not found: {args.file}')
    with open(args.file, 'r') as fh:
        configuration = Configuration.from_dict(json.load(fh))

    worker_submission = configuration.worker

    master = ScaleMSMaster(configuration)

    master.submit(**dataclasses.asdict(worker_submission))

    master.start()
    master.join()
    master.stop()


class ScaleMSWorker(rp.raptor.Worker):
    def __init__(self, cfg: str):
        # *cfg* is a JSON file generated by the Master to provide a
        # TaskDescription-like record to the raptor.Worker initializer.
        rp.raptor.Worker.__init__(self, cfg)

        self.register_mode('gmx', self._gmx)

    def _gmx(self, data):
        out = 'gmx  : %s %s' % (time.time(), data['blob'])
        err = None
        ret = 0

        return out, err, ret

    def hello(self, world):
        return 'call : %s %s' % (time.time(), world)


def worker():
    """Manage the life of a Worker instance.

    Launched as a task submitted with the worker_descr provided to the corresponding
    Master script.
    The framework generates a local file, passed as the first argument to the script,
    which is processed by the raptor.Worker base class for initialization.

    The Worker instance created here will receive Requests as dictionaries
    on a queue processed with Worker._request_cb, which then passes requests
    to Worker._dispatch functions in forked interpreters.

    Worker._dispatch handles requests according to the *mode* field of the request.
    New *mode* names may be registered with Worker.register_mode, but note that
    the important place for the mode to be registered is at the forked interpreter,
    not the interpreter running this main() function.
    """
    if not os.environ['RP_SESSION_ID']:
        raise RuntimeError('Raptor Worker must be launched by RP executor.')

    # Master generates a file to be appended to the argument list.
    #
    cfg = None
    if len(sys.argv) > 1:
        cfg = sys.argv[1]

    worker = ScaleMSWorker(cfg=cfg)
    worker.start()
    time.sleep(5)
    worker.join()
