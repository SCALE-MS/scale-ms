"""Intra-component communication support.

Message types and protocol support for control signals,
queue management, and work dispatching.
"""
__all__ = ("Command", "QueueItem", "CommandQueueControlItem", "CommandQueueAddItem", "StopCommand")

import logging
import typing
import weakref
from abc import abstractmethod

from scalems.exceptions import APIError

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class QueueItem(dict, typing.MutableMapping[str, typing.Union[str, bytes]]):
    """Queue items are either workflow items or control messages.

    Control messages are indicated by the key ``'command'``, and are described
    by :py:class:`CommandQueueControlItem`.

    Workflow items are indicated by the key ``'add_item'``, and are described by
    :py:class:`CommandQueueAddItem`.
    """

    def _hexify(self):
        """Allow binary fields to be printable."""
        for key, value in self.items():
            if isinstance(value, bytes):
                value = value.hex()
            yield key, value

    def __str__(self) -> str:
        return str(dict(self._hexify()))


class CommandQueueControlItem(QueueItem, typing.MutableMapping[str, str]):
    """String-encoded Command for the Executor command queue.

    Instructions for the `RuntimeManager`, intercepted and processed by
    :py:func:`scalems.execution.manage_execution()`.

    Currently, the only supported key is "command".

    Supported commands may grow to comprise a Compute Provide Interface.
    """

    _allowed: typing.ClassVar = {"command": {"hello", "stop", "version"}}

    def __setitem__(self, k: str, v: str) -> None:
        if k in self._allowed:
            if v in self._allowed[k]:
                super().__setitem__(k, v)
            else:
                raise APIError(f"Unsupported command value: {repr(v)}")
        else:
            raise APIError(f"Unsupported command key: {repr(k)}")


class CommandQueueAddItem(QueueItem, typing.MutableMapping[str, bytes]):
    """String-encoded add_item command for the Executor command queue.

    The intended payload is an item to be added to the workflow graph:
    e.g. an operation, data reference, subgraph, or something meaningful to an
    :py:class:`~scalems.execution.AbstractWorkflowUpdater`.

    In practice, the value may just be a token or identifier. In the initial
    version of the scalems protocol, the value is passed to the item editor factory
    that is obtained from :py:func:`scalems.execution.RuntimeManager.get_edit_item()`.
    """

    _allowed: typing.ClassVar = {"add_item"}

    def __setitem__(self, k: str, v: bytes) -> None:
        if k in self._allowed:
            if isinstance(v, bytes):
                super().__setitem__(k, v)
            else:
                raise APIError(f"Unsupported add_item key: {repr(v)}")
        else:
            raise APIError(f"Unsupported command: {repr(k)}")


SerializedValueT = typing.TypeVar("SerializedValueT", bound=typing.Union[str, bytes])


class Command(typing.Protocol[SerializedValueT]):
    """An instruction to the workflow execution run time machinery.

    A limited schema for Command objects allows abstraction of the workflow
    management and run time collaborations to be represented in a way that is
    wire-protocol-friendly, easily serializable, and easily converted between
    structured data types or file formats.

    Commands of different types (represented by subclasses of :py:class:`~scalems.messages.Command`)
    are directed to different components of the SCALE-MS workflow management model.

    Support for serialization follows the model of the object encoding/decoding
    hooks in the :py:mod:`json` module.

    Immediate realizations of the Command Protocol are Abstract Base Classes
    that map themselves to "command category" strings, which serve as keys in
    the serialized mapping object representation.

    The Abstract Base Classes are responsible for translating the serialized
    object value into a Command subclass instance.
    """

    key: typing.ClassVar[str]
    """Key word for mapping a named command to the Command creation function."""

    # TODO(#23): This class hierarchy needs rethinking.
    # This is a reasonable attribute of an Abstract Base Class,
    # but should not be part of a Protocol definition since it is not a behavior
    # that we want to specify for all instances.
    __subtype: typing.ClassVar[
        typing.MutableMapping[str, typing.Callable[..., typing.Callable[[SerializedValueT], typing.Any]]]
    ] = {}
    """Map named command categories to WeakMethod weakrefs to callable decoders."""

    @classmethod
    @abstractmethod
    def create(cls, command: SerializedValueT) -> "Command":
        """Creation function will be registered during subclassing.

        The *command* argument will be provided with the *value*
        of the deserialized Mapping entry during *decode()*.
        """
        raise NotImplementedError

    @staticmethod
    def decode(obj: typing.Mapping[str, SerializedValueT]) -> "Command":
        """Convert a deserialized object representation to a Command instance.

        Abstract Command base classes
        """
        items = tuple(obj.items())
        if len(items) != 1 or len(items[0]) != 2:
            raise ValueError("Not a key-value pair. decode() expects a (single-element) Mapping.")
        key, value = items[0]
        creation_function = Command.__subtype[key]()
        return creation_function(value)

    @abstractmethod
    def encode(self):
        """Produce a serializeable object record."""
        # Example:
        #   return {self.key: self.value}
        raise NotImplementedError

    def __init_subclass__(cls, **kwargs):
        """Register an abstract command base class for dispatched creation."""
        # Check the protocol.
        if cls is not Command:
            assert hasattr(cls, "key")
            assert cls.key not in Command.__subtype
            Command.__subtype[cls.key] = weakref.WeakMethod(cls.create)
            super().__init_subclass__(**kwargs)


class Control(Command[str], typing.Protocol):
    """Represent a "control" message.

    Supported commands for runtime components.

    Abstract Base Class for Control commands.
    Serialized Control objects are identified by the key word "control".
    The payload (or serialized object) is just a string naming the control command.

    Note: I'm not sure that it is appropriate for concrete Control types to be
    subclasses of Control, and, thus, Command. It is convenient metaprogramming,
    but what we want is more akin to the `register` interaction of explicitly _virtual_
    subclasses, or of the dispatching function objects from functools. I'm withholding
    judgement or redesign until seeing how things play out with command types that
    take arguments, or more complete functionality like dispatchable tasks. At that
    point, though, I expect a decorator pattern will make more sense.
    """

    key: typing.ClassVar = "control"

    message: typing.ClassVar[str]

    # TODO(#23): This class hierarchy needs rethinking.
    # This is a reasonable attribute of an Abstract Base Class,
    # but should not be part of a Protocol definition since it is not a behavior
    # that we want to specify for all instances.
    __control_type: typing.ClassVar[typing.MutableMapping[str, type]] = weakref.WeakValueDictionary()
    """Map named control commands to their concrete types."""

    def __init_subclass__(cls, **kwargs):
        assert hasattr(cls, "message")
        assert cls.message not in Control.__control_type
        Control.__control_type[cls.message] = cls
        # The Control abstract base class has separate semi-private relationships
        # with its parents and with its children. Control insulates subclasses
        # from other base classes, and we interrupt the init_subclass protocol
        # here.
        # Do not call super().__init_subclass__(**kwargs)

    @classmethod
    def create(cls, command: str) -> "Command":
        assert cls is Control
        return cls.__control_type[command]()

    def encode(self):
        return {self.key: self.message}


class StopCommand(Control):
    message: typing.ClassVar[str] = "stop"
    """Announce the end of the session, directing components to shut down cleanly.

    No further commands will be processed after receiving this signal.
    """

    def __init__(self):
        # With a Protocol parent, we need an __init__ to establish this as a concrete class.
        ...


class HelloCommand(Control):
    message: typing.ClassVar[str] = "hello"
    """Elicit a response.

    The structure of the response is a detail of the backend, but probably
    includes version and configuration information to support compatibility
    checks.
    """

    def __init__(self):
        # With a Protocol parent, we need an __init__ to establish this as a concrete class.
        ...


class AddItem(Command[str]):
    """Announce an item to be added to the target workflow.

    Direct an update to the workflow record, such as to describe a data source,
    register a resource, or add a task.

    The payload is a json-serialized ScalemsRaptorWorkItem. An instance can
    be provided as an initializer argument for a backend specific CpiAddItem.

    Design note:
        WorkflowManager.add_item() and CommandQueueAddItem are not normalized or
        normative. We can start engineering scalems.messages.AddItem
        and scalems.radical.raptor.CpiAddItem together from the bottom up and then
        make another pass at the higher level / entry point interface.

    """

    key: typing.ClassVar = "add_item"

    def __init__(self, encoded_item: str):
        # TODO: This should be a public scalems schema, but is currently assumed
        #  to be scalems.radical.raptor.ScalemsRaptorWorkItem.
        self._encoded_item = encoded_item

    @classmethod
    def create(cls, command: str) -> "Command":
        return cls(command)

    def encode(self):
        return {self.key: self._encoded_item}

    @property
    def encoded_item(self):
        return self._encoded_item
