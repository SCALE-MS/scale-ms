"""Define the connective tissue for SCALE-MS tasks embedded in rp.Task arguments.

This module should not depend on any scalems modules.

Client:
    The client should be reasonably certain that the target environment has a
    compatible installation of RP and scalems. A rp.raptor.Master task script and a
    rp.raptor.Worker task script are installed with the scalems package. Their names
    are returned by the module functions ``master_script()`` and ``worker_script()``,
    and they should be resolvable on the PATH for a Python interpreter process in an
    environment that has the scalems package installed.

"""
import dataclasses
import functools
import typing

try:
    import radical.pilot as rp
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

_RaptorReturnType = typing.Tuple[
    typing.Union[None, typing.Text],
    typing.Union[None, typing.Text],
    typing.Union[None, typing.SupportsInt],
    typing.Any
]
"""Raptor worker task return values are interpreted as a tuple (out, err, ret).

The first two elements are cast to output and error strings, respectively.

The third is cast to an integer return code.

The fourth is a return value of arbitrary type.
"""

_RaptorWorkData = typing.TypeVar('_RaptorWorkData')
"""Argument type for a Raptor task implementation.

Constraints on the data type are not yet well-defined.
Presumably, the object must be "Munch"-able.
"""


_DataT = typing.TypeVar('_DataT')


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


class _RequestInput(typing.TypedDict):
    """Input argument for a raptor.Request instantiation.

    Not yet fully specified, but not to be confused with
    raptor.Request instances.

    A dict-like object with at least a *uid* key.

    As of RP 1.6.7, the RequestInput is dict-like object deserialized
    from the wrapper TaskDescription *arguments* member's first element.
    After deserialization, the dict is assigned values for *is_task*,
    *uid*, and *task*.

    The mapping is assumed to contain keys *mode*, *data*, and *timeout*
    when it becomes incorporated into a new Request object (as the core
    data member) after Master.request_cb(). Optionally, it may contain
    *cores* and *gpus*.
    """
    uid: str
    mode: str
    data: typing.Any
    timeout: typing.SupportsFloat
    cores: typing.Optional[int]
    gpus: typing.Optional[int]


class ScaleMSRequestInput(_RequestInput, ScaleMSTaskDescription):
    """"""


class RaptorWorkCallable(typing.Protocol[_RaptorWorkData]):
    def __call__(self, data: _RaptorWorkData) -> _RaptorReturnType:
        ...


class RaptorWorkDescription(typing.Protocol[_RaptorWorkData]):
    """Represent the content of an *arguments* element in a RaptorTaskDescription.

    A dictionary resembling this structure is converted to radical.pilot.raptor.Request
    by the Master in radical.pilot.raptor.Master.request().

    Note that some keys may be added or overwritten during Master._receive_tasks
    (e.g. *is_task*, *uid*, *task*).
    """
    cores: int

    timeout: typing.SupportsFloat

    mode: str
    """Dispatching key for raptor.Worker._dispatch()

    Must map to a mode (RaptorWorkCallable) in the receiving Worker._modes
    """

    data: _RaptorWorkData
    """Munch-able object to be passed to Worker._modes[*mode*](*data*)."""


class _RaptorTaskDescription(typing.Protocol):
    """Note the distinctions of a TaskDescription to processed by a raptor.Master.

    The single element of *arguments* is a JSON-encoded object that will be
    deserialized (RaptorWorkDescription) as the prototype for the dictionary used to instantiate the Request.
    """
    uid: str  # Unique identifier for the Task across the Session.
    executable: typing.ClassVar[str] = 'scalems'  # Unused by Raptor tasks.
    scheduler: str  # The UID of the raptor.Master scheduler task.
    arguments: typing.Sequence[str]  # Processed by raptor.Master._receive_tasks


class _RaptorWorkerTaskDescription(typing.Protocol):
    """rp.TaskDescription for Scalems raptor.Worker tasks.

    Note that this is just a rp.TaskDescription.
    """
    uid: str  # Unique identifier for the Task across the Session.
    executable: typing.ClassVar[str]  # scalems_rp_worker script.
    scheduler: str  # The UID of the raptor.Master scheduler task.
    arguments: typing.Sequence[str]  # Processed by raptor.Master._receive_tasks
    pre_exec: typing.List[str]
    # Other rp.TaskDescription fields are available, but unused.
    # pre_launch: typing.List[str]
    # pre_rank: typing.Mapping[int, typing.List[str]]
    # post_exec: typing.List[str]
    # post_launch: typing.List[str]
    # post_rank: typing.Mapping[int, typing.List[str]]


class RaptorWorkerTaskDescription(_RaptorWorkerTaskDescription, rp.TaskDescription):
    def __init__(self, *args, from_dict=None, **kwargs):
        if from_dict is None:
            from_dict = dict(*args, **kwargs)
        else:
            if len(args) or len(kwargs):
                raise TypeError('Use only one of the dict signature or the '
                                'TaskDescription signature.')
        rp.TaskDescription.__init__(self, from_dict=from_dict)


class RaptorWorkerConfigDict(typing.TypedDict):
    """Signature of the rp.raptor.Master.submit()"""
    descr: _RaptorWorkerTaskDescription
    count: typing.Optional[int]
    cores: typing.Optional[int]
    gpus: typing.Optional[int]


@dataclasses.dataclass
class RaptorWorkerConfig:
    """Signature of the rp.raptor.Master.submit()"""
    count: typing.Optional[int]
    cores: typing.Optional[int]
    gpus: typing.Optional[int]
    descr: RaptorWorkerTaskDescription = dataclasses.field(
        default_factory=RaptorWorkerTaskDescription)

    @classmethod
    def from_dict(cls: 'RaptorWorkerConfig', obj: RaptorWorkerConfigDict) -> 'RaptorWorkerConfig':
        return cls(
            descr=RaptorWorkerTaskDescription(from_dict=obj['descr']),
            count=obj['count'],
            cores=obj['cores'],
            gpus=obj['gpus'],
        )


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
    def from_dict(cls: typing.Type['Configuration'], obj: _ConfigurationDict) -> 'Configuration':
        return cls(
            worker=RaptorWorkerConfig.from_dict(obj['worker']),
            versioned_modules=list(obj['versioned_modules'])
        )


@object_encoder.register
def _(obj: Configuration) -> dict:
    return dataclasses.asdict(obj)


RequestInputList = typing.List[_RequestInput]


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
