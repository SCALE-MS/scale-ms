"""Common interfaces for scalems and radical.pilot.

Define the connective tissue for SCALE-MS tasks embedded in rp.Task arguments.

This module should not depend on any scalems modules, and ultimately may disappear as
the RP.raptor interfaces are formalized.
"""

__all__ = (
    'Request',
    'RequestInput',
    'RequestInputList',
    'RaptorWorkerConfig',
    'RaptorWorkerConfigDict',
    'RaptorWorkerTaskDescription',
    'RaptorWorkDescription',
    'RaptorWorkCallable'
)

import dataclasses
import typing

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


class RequestInput(typing.TypedDict):
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
    deserialized (RaptorWorkDescription) as the prototype for the dictionary used to
    instantiate the Request.
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
    def from_dict(cls: 'RaptorWorkerConfig',
                  obj: RaptorWorkerConfigDict) -> 'RaptorWorkerConfig':
        return cls(
            descr=RaptorWorkerTaskDescription(from_dict=obj['descr']),
            count=obj['count'],
            cores=obj['cores'],
            gpus=obj['gpus'],
        )


RequestInputList = typing.List[RequestInput]
