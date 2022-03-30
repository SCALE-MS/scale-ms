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
import typing
from importlib.machinery import ModuleSpec
from importlib.util import find_spec

import packaging.version

from .common import RaptorWorkerConfig
from .common import rp
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

api_name = 'scalems_v0'
"""Key for dispatching raptor Requests.

Provided to `rp.raptor.Worker.register_mode()`, this module constant can provide some
compatibility checking when receiving requests.
"""


# As of RP 1.14, tasks destined for raptor Workers have new schema and requirements.
# TODO: document/check schema for raptor tasks (mode-dependent required and optional fields)
# class ScaleMSTaskDescription(typing.TypedDict):
#     """SCALE-MS specialization of extra raptor task info.
#
#     This structure is serialized into the TaskDescription.arguments[0] of a rp.Task
#     submitted with a *scheduler* set to a ``scalems_rp_master`` Task uid.
#
#     When deserialized, the object is treated as the prototype for a _RequestInput.
#
#     The mapping is assumed to contain keys *mode*, *data*, and *timeout*
#     when it becomes incorporated into a new Request object (as the core
#     data member) after Master.request_cb(). Optionally, it may contain
#     *cores* and *gpus*.
#     """
#     mode: str
#     data: typing.Any
#     timeout: typing.SupportsFloat
#     cores: typing.Optional[int]
#     gpus: typing.Optional[int]


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

    def result_cb(self, requests: typing.Sequence[rp.TaskDescription]):
        """SCALE-MS specific handling of tasks completed by the collaborating Worker(s).

        .. todo:: Clarify TaskDescription fields that are unique to raptor tasks, and the details of the
            result tuple.
        """
        for r in requests:
            logger.info('result_cb %s: %s [%s]' % (r.uid, r.state, r.result))

    def request_cb(
            self,
            requests: typing.Sequence[rp.TaskDescription]
    ) -> typing.Sequence[rp.TaskDescription]:
        """Allows all incoming requests to be processed by the Master.

        Request is a (dictionary? rp.TaskDescription instance?)... (additional keys?)

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
        # for req_input in requests:
        #     request = Request(req=req_input)
        #     # possibly
        #     self.request(request)
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
    if not os.environ['RP_TASK_ID']:
        raise RuntimeError('Raptor Master must be launched by RP executor.')

    args = parser.parse_args()
    if not os.path.exists(args.file):
        raise RuntimeError(f'File not found: {args.file}')
    with open(args.file, 'r') as fh:
        configuration = Configuration.from_dict(json.load(fh))

    worker_submission = configuration.worker

    _master = ScaleMSMaster(configuration)

    _master.submit_workers(**worker_submission)

    _master.start()
    _master.join()
    _master.stop()

# TODO: Inject just a scalems dispatching function, pickled into a radical.pilot.PythonTask.pythontask
# class ScaleMSWorker(rp.raptor.DefaultWorker):
#     try:
#         from scalems.radical.raptor import api_name as _sms_api_name
#     except ImportError:
#         _sms_api_name = None
#
#     # TODO: Confirm that signature satisfies the expected signature of *executor* in Worker.register_mode()
#     def handle_request(self: rp.raptor.Worker, data: dict) -> RaptorReturnType:
#         # We assume this handler is called by virtue of having been registered for the
#         # *mode* named from *scalems.radical.raptor.api_name* as a bound method of a
#         # rp.raptor.Worker subclass.
#         # We can attempt to deserialize *data* to a registered scalems request type and
#         # dispatch to the handler for that type.
#         # Check api version field against self._sms_api_name
#         ...
#
#
# def worker():
#     """Manage the life of a Worker instance.
#
#     Launched as a task submitted with the worker_descr provided to the corresponding
#     Master script.
#     The framework generates a local file, passed as the first argument to the script,
#     which is processed by the raptor.Worker base class for initialization.
#
#     The Worker instance created here will receive Requests as dictionaries
#     on a queue processed with Worker._request_cb, which then passes requests
#     to Worker._dispatch functions in forked interpreters.
#
#     Worker._dispatch handles requests according to the *mode* field of the request.
#
#     RP 1.14+
#     --------
#     In RP 1.14+, we will use the rp.TASK_FUNCTION *mode* to direct the launching of
#     the callable named in the *function* field. The *args* and *kwargs* fields
#     provide the parameters to the callable.
#     """
#     if not os.environ['RP_TASK_ID']:
#         raise RuntimeError('Raptor Worker must be launched by RP executor.')
#
#     # Master generates a file to be appended to the argument list.
#     #
#     cfg = None
#     if len(sys.argv) > 1:
#         cfg = sys.argv[1]
#
#     worker = ScaleMSWorker(cfg=cfg)
#     worker.start()
#     time.sleep(5)
#     worker.join()
