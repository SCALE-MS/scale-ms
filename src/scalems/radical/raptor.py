"""Support for scalems on radical.pilot.raptor.

Client:
    The client should be reasonably certain that the target environment has a
    compatible installation of RP and scalems. A rp.raptor.Master task script is
    installed with the scalems package. The script name is provide by the
    module function :py:func:`~scalems.radical.raptor.master_script()`, and will
    be resolvable on the PATH for a Python interpreter process in an
    environment that has the `scalems` package installed.

"""
import argparse
import dataclasses
import functools
import importlib
import importlib.metadata
import json
# We import rp before `logging` to avoid warnings when rp monkey-patches the
# logging module.
import logging
import os
import typing
from importlib.machinery import ModuleSpec
from importlib.util import find_spec

import packaging.version

from .common import RaptorWorkerConfig
from .common import rp
from .common import TaskDictionary

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)

api_name = 'scalems_v0'
"""Key for dispatching raptor Requests.

We can use this to identify the schema used for SCALE-MS tasks encoded in
arguments to raptor *call* mode executor functions.
"""


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
    worker: RaptorWorkerConfig
    versioned_modules: typing.List[typing.Tuple[str, str]]


# @object_encoder.register
# def _(obj: rp.TaskDescription) -> dict:
#     return obj.as_dict()


# TODO: Disambiguate from scalems.radical.runtime.Configuration.
@dataclasses.dataclass
class Configuration:
    """Input to the script responsible for the RP raptor Master.

    .. todo:: Check the schema for RP 1.14
    """
    worker: RaptorWorkerConfig
    versioned_modules: typing.List[typing.Tuple[str, str]]

    @classmethod
    def from_dict(cls: typing.Type['Configuration'],
                  obj: _ConfigurationDict) -> 'Configuration':
        return cls(
            worker=RaptorWorkerConfig(**obj['worker']),
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
    _master_script = 'scalems_rp_master'
    if pkg_resources is not None:
        # It is not hugely important if we cannot perform this test.
        # In reality, this should be performed at the execution site, and we can/should
        # remove the check here once we have effective API compatibility checking.
        # See https://github.com/SCALE-MS/scale-ms/issues/100
        assert pkg_resources.get_entry_info('scalems', 'console_scripts',
                                            'scalems_rp_master').name == _master_script
    return _master_script


class SoftwareCompatibilityError(RuntimeError):
    """Incompatible package versions or software interfaces."""


def _get_module_version(module: str):
    """Get version metadata for importable module."""
    try:
        found_version = importlib.metadata.version(module)
    except importlib.metadata.PackageNotFoundError:
        found_version = None
    if found_version is None:
        try:
            spec: ModuleSpec = find_spec(module)
            if spec and hasattr(spec, 'parent'):
                found_version = importlib.metadata.version(spec.parent)
        except Exception as e:
            logger.debug(f'Exception when trying to find {module}: ', exc_info=e)
    if found_version is not None:
        found_version = packaging.version.Version(found_version)
    return found_version


class ScaleMSMaster(rp.raptor.Master):
    def __init__(self, configuration: Configuration):
        for module, version in configuration.versioned_modules:
            logger.debug(f'Looking for {module} version {version}.')
            found_version = _get_module_version(module)
            if found_version is None:
                raise SoftwareCompatibilityError(f'{module} not found.')
            minimum_version = packaging.version.Version(version)
            if found_version >= minimum_version:
                logger.debug(f'Found {module} version {found_version}: Okay.')
            else:
                raise SoftwareCompatibilityError(
                    f'{module} version {found_version} not compatible with {version}.'
                )
        kwargs = {}
        rp_version = packaging.version.parse(rp.version_short)
        if rp_version < packaging.version.Version('1.15'):
            kwargs['cfg'] = {}
        super(ScaleMSMaster, self).__init__(**kwargs)

    def result_cb(self, tasks: typing.Sequence[TaskDictionary]):
        """SCALE-MS specific handling of tasks completed by the collaborating Worker(s).

        Notes:
            The RP task dictionary described is for the MPIWorker in RP 1.14. It will be
            normalized in a future RP release.
        """
        for r in tasks:
            logger.info('result_cb %s: %s [%s]' % (r['uid'], r['state'], str(r['val'])))

    def request_cb(
            self,
            tasks: typing.Sequence[TaskDictionary]
    ) -> typing.Sequence[TaskDictionary]:
        """Allows all incoming requests to be processed by the Master.

        Note that Tasks may not be processed in the same order in which they are submitted
        by the client.

        If overridden, request_cb() must return a :py:class:`list`. The returned list
        is interpreted to be requests that should be processed normally after the callback.
        This allows subclasses of rp.raptor.Master to add or remove requests before they become Tasks.
        The returned list is submitted with self.request() by the base class after the
        callback returns.

        A Master may call *self.request()* to self-submit items (e.g. instead of or in
        addition to manipulating the returned list in *request_cb()*).

        It is the developer's responsibility to choose unique task IDs (uid) when crafting
        items for Master.request().
        """
        return tasks


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
    if not os.environ['RP_TASK_ID']:
        raise RuntimeError('Raptor Master must be launched by RP executor.')

    args = parser.parse_args()
    if not os.path.exists(args.file):
        raise RuntimeError(f'File not found: {args.file}')
    with open(args.file, 'r') as fh:
        configuration = Configuration.from_dict(json.load(fh))

    # TODO: Configurable log level?
    logger.setLevel(logging.DEBUG)
    character_stream = logging.StreamHandler()
    character_stream.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    character_stream.setFormatter(formatter)
    logger.addHandler(character_stream)

    logger.info(f'Launching ScaleMSMaster task with {repr(dataclasses.asdict(configuration))}.')

    worker_submission = configuration.worker

    _master = ScaleMSMaster(configuration)
    logger.debug(f'Created {repr(_master)}.')

    _master.submit_workers(**worker_submission)
    _workers = list(_master._workers.keys())
    message = 'Submitted workers: '
    message += ', '.join(_workers)
    logger.info(message)

    _master.start()
    logger.debug('Master started.')

    # TODO: Confirm workers start successfully or produce useful error.

    _master.join()
    logger.debug('Master joined.')
    _master.stop()
    logger.debug('Master stopped.')


def dispatch(*args, comm=None, **kwargs):
    """Unpack and run a task requested through RP Raptor.

    To be implemented in resolution of #108.

    Args:
        *args: list of positional arguments from *args* in the TaskDescription.

    Keyword Args:
        comm (mpi4py.MPI.Comm, optional): MPI communicator to be used for the task.
        **kwargs: dictionary of keyword arguments from *kwargs* in the TaskDescription.

    See Also:
        `scalems.radical.common.scalems_dispatch()`
    """
    ...
