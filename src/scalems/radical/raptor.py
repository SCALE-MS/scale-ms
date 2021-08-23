"""
Define the connective tissue for SCALE-MS tasks embedded in rp.Task arguments.
"""

import typing

import radical.pilot as rp


_RaptorReturnType = typing.Tuple[typing.Text, typing.Text, typing.SupportsInt]
"""Raptor worker task return values are interpreted as a tuple (out, err, ret).

The first two elements are cast to output and error strings, respectively.

The third is cast to an integer return code.
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
    is_task: bool
    uid: str
    task: rp.Task
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

    Attributes:
        mode (str): Dispatching key for raptor.Worker._dispatch()

    Note that some keys may be added or overwritten during Master._receive_tasks
    (e.g. *is_task*, *uid*, *task*).
    """
    cores: int
    timeout: typing.SupportsFloat
    mode: str  # Must map to a mode (RaptorWorkCallable) in the receiving Worker._modes
    data: _RaptorWorkData  # Munch-able object to be passed to Worker._modes[*mode*](*data*)


class _RaptorTaskDescription(typing.Protocol):
    """Note the distinctions of a TaskDescription to processed by a raptor.Master.


    The single element of *arguments* is a JSON-encoded object that will be
    deserialized (RaptorWorkDescription) as the prototype for the dictionary used to instantiate the Request.
    """
    uid: str  # Unique identifier for the Task across the Session.
    executable: typing.ClassVar[str] = 'scalems'  # Unused by Raptor tasks.
    scheduler: str  # The UID of the raptor.Master scheduler task.
    arguments: typing.Sequence[str]  # Processed by raptor.Master._receive_tasks


RequestInputList = typing.List[_RequestInput]
