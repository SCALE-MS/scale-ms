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
from __future__ import annotations

__all__ = []

import abc
import collections
import json
import os
import typing
import weakref

from scalems.core.exceptions import ProtocolError

json_base_encodable_types: typing.Tuple[type, ...] = (dict, list, tuple, str, int, float, bool, type(None))
json_base_decoded_types: typing.Tuple[type, ...] = (dict, list, str, int, float, bool, type(None))

BaseEncodable = typing.Union[dict, list, tuple, str, int, float, bool, None]


DispatchT = typing.TypeVar('DispatchT')

class DispatchingFunction(typing.Protocol[ResultT]):
    def register(self, dtype: typing.Type[DispatchT], handler: typing.Callable[[DispatchT], ResultT]):
        ...

class PythonEncoder:
    """Encode SCALE-MS objects as basic Python data that is easily serialized.

    Extend the JSONEncoder for representations in the SCALE-MS data model by
    passing to the *default* argument of ``json.dumps()``,
    but note that it will only be used for objects that JSONEncoder does not already
    resolve.

    Note that json.dump and json.dumps only use the *default* call-back when the *cls* encoder does not
    have an implementation for an object type. To preempt standard processing by the JSONEncoder,
    you must provide a *cls* that overrides the encoding methods as documented at
    https://docs.python.org/3/library/json.html#json.JSONEncoder.encode to produce a string.
    This is _not_ what the *encode* method of this class does.

    Alternatively, encode object(s) first, and pass the resulting Python object to a regular call to json.dumps.
    """
    # Note that the following are equivalent.
    #     json.loads(s, *, cls=None, **kw)
    #     json.JSONDecoder(**kw).decode(s)
    # Note that the following are equivalent.
    #     json.dumps(obj, *, cls=None, **kw)
    #     json.JSONEncoder(**kw).encode(obj)

    # We use WeakKeyDictionary because the keys are likely to be classes,
    # and we don't intend to extend the life of the type objects (which might be temporary).
    _dispatchers: typing.ClassVar[typing.MutableMapping[
        typing.Type[DispatchT], typing.Callable[[DispatchT], BaseEncodable]]] = weakref.WeakKeyDictionary()

    @classmethod
    def register(cls, dtype: typing.Type[DispatchT], handler: typing.Callable[[DispatchT], BaseEncodable]):
        # Note that we don't expect references to bound methods to extend the life of the type.
        # TODO: confirm this assumption in a unit test.
        if dtype in cls._dispatchers:
            raise ProtocolError('Encodable type appears to be registered already.')
        cls._dispatchers[dtype] = handler

    @classmethod
    def unregister(cls, dtype: typing.Type[DispatchT]):
        # As long as we use a WeakKeyDictionary, explicit unregistration should not be necessary.
        del cls._dispatchers[dtype]

    @classmethod
    def encode(cls, obj) -> BaseEncodable:
        """Convert an object of a registered type to a representation as a basic Python object."""
        # Currently, we iterate because we may be using abstract types for encoding.
        # If we find that we are using concrete types and/or we need more performance,
        # or if we just find that the list gets enormous, we can inspect the object first
        # to derive a dtype key that we can look up directly.
        # Warning: we should be careful not to let objects unexpectedly match multiple entries.
        for dtype, dispatch in cls._dispatchers.items():
            if isinstance(obj, dtype):
                return dispatch(obj)
        raise TypeError(f'No registered dispatching for {repr(obj)}')

    def __call__(self, obj) -> BaseEncodable:
        return self.encode(obj)


encode = PythonEncoder()

# TODO: use stronger check for UID, or bytes-based objects.
encode.register(dtype=bytes, handler=bytes.hex)
encode.register(dtype=os.PathLike, handler=os.fsdecode)


# decode: DispatchingFunction[typing.Any] = _customdispatch(
#     docstring="""Decode SCALE-MS objects from Python dictionaries.
#
#     Extend the JSONDecoder for representations in the SCALE-MS data model by
#     passing to the *object_hook* argument of ``json.loads()``.
#     """)


class Decoder:
    """Convert dictionary representations to SCALE-MS objects for registered types."""


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
