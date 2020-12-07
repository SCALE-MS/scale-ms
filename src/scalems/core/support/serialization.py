"""Provide encoding and decoding support for serialized workflow representations.

Reference https://docs.python.org/3/library/json.html#json.JSONDecoder and
https://docs.python.org/3/library/json.html#py-to-json-table describe the
trivial Python object conversions.
The core SCALE-MS encoder / decoder needs to manage the conversion of
additional types (scalems or otherwise, e.g. *bytes*) to/from these basic
Python types.

For JSON, we can provide an encoder for the *cls* parameter of json.dumps()
and we can provide a key-value pair processing dispatcher to the *object_pairs_hook*
parameter of json.loads()

We may also implement serialization schemes for document formats other than
JSON, such as the CWL schema.
"""
import abc
import collections
import json
import os
import typing


json_base_encodable_types: typing.Tuple[type, ...] = (dict, list, tuple, str, int, float, bool, type(None))
json_base_decoded_types: typing.Tuple[type, ...] = (dict, list, str, int, float, bool, type(None))


BaseEncodable = typing.Union[dict, list, tuple, str, int, float, bool, type(None)]


class JsonObjectPairsDispatcher:
    """Decode key/value pairs from JSON objects into SCALE-MS objects.

    Provides a place to register different type handlers.

    Each JSON *object* deserialized by the JSON Decoder is passed as a sequence
    of (key, value) pairs. The result is returned instead of the usual *dict(pairs)*.

    We don't have interaction with state or nesting, so we may have to examine the
    values for Python objects that have already been decoded to see if additional
    processing is necessary once the context of the key/value pair is known.
    """
    def __call__(self, *args, **kwargs):
        ...


S = typing.TypeVar('S')


class Serializable(abc.ABC):
    """Base class for serialization behaviors.

    Subclassing takes care of registering encoders and decoders at
    module import.

    Serializable types must support encoding to and decoding from
    a small set of basic Python types for serialization schemes,
    such as JSON.
    """
    @abc.abstractmethod
    def encode(self) -> typing.Union[dict, list, tuple, str, int, float, bool, type(None)]:
        ...

    @classmethod
    @abc.abstractmethod
    def decode(cls: typing.Type[S], *args, **kwargs) -> S:
        ...

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Create a weakref and add to Encoder._encoders with a finalizer to remove from same.
        # Similarly add to the Decoder dispatcher...


# The most fundamental type we need to add serialization support for is the UID,
# which has a *bytes* representation in Python.
class UID(Serializable, bytes):
    """Support for the *uid* field of workflow objects."""



class Encoder(json.JSONEncoder):
    """Extend the JSONEncoder for representations in the SCALE-MS data model."""
    _encoders = {}

    def default(self, o):
        if isinstance(o, bytes):
            # TODO: use stronger check for UID, or bytes-based objects.
            return o.hex()
        if isinstance(o, os.PathLike):
            return os.fsdecode(o)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, o)


Key = typing.Union[str, int, slice]


def _random_uid():
    """Generate a random (invalid) UID, such as for testing."""
    import hashlib as _hashlib
    from random import randint
    return _hashlib.sha256(randint(0, 2**255).to_bytes(32, byteorder='big')).digest()


class OperationIdentifier(tuple):
    """Python structure to identify an API Operation implementation.

    Operations are identified with a nested scope. The OperationIdentifier
    is a sequence of identifiers such that the operation_name() is the final
    element, and the preceding subsequence comprises the namespace().

    Conventional string representation of the entire identifier uses a period
    (``.``) delimiter.
    """
    def namespace(self):
        return tuple(self[0:-2])

    def operation_name(self):
        return self[-1]

    def __str__(self):
        return '.'.join(self)


class OperationNode(abc.ABC):
    """Abstract interface for operation node references."""
    @abc.abstractmethod
    def to_json(self, **json_args) -> str:
        """Serialize the node to a JSON record."""
        ...

    @classmethod
    def from_json(cls, serialized: str):
        """Creation method to deserialize a JSON record."""
        # TODO: We could, of course, dispatch to registered subclasses,
        #  but this is deferred until refactoring converts node references into
        #  views into a Context.
        ...

    @abc.abstractmethod
    def fingerprint(self):
        """Get the unique identifying information for the node."""
        ...
