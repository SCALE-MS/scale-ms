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
import weakref


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


# The terminology is potentially a bit confusing.
# We might want to clarify that we have a PythonEncoder and PythonDecoder that convert
# between scalems objects and native Python objects (that are JSON-encodable),
# and a JsonSerializer / JsonDeserializer that convert to and from JSON document strings.
# Note that the following are equivalent.
#     json.loads(s, *, cls=None, **kw)
#     json.JSONDecoder(**kw).decode(s)
# Note that the following are equivalent.
#     json.dumps(obj, *, **kw)
#     json.JSONEncoder(**kw).encode(obj)


# TODO: Can we clean up the following?
# It seems to be difficult to get static type checking and the various docstring extractors to all work well together.
# We want the module member "functions" to have the right docstrings, but we want their registration methods to have
# type-checkable signatures. The initial implementation below is probably more complex than it needs to be, but I gave
# up after a while and stuck with the last working version.
#
# We probably don't actually need the generality. Probably just define a class with class methods.
# If we need the generality, we can inherit from the class.
# If we don't like the PEP-8 warning about a class name with an uncapitalized first letter,
# we can import Encoder as `encode` in the package module that is supposed to actually export it.

TypeArg = typing.TypeVar('TypeArg', bound=typing.Hashable)
DispatchT = typing.TypeVar('DispatchT')
T = typing.TypeVar('T')
ResultT = typing.TypeVar('ResultT')


class DispatchingFunction(typing.Protocol[ResultT]):
    def register(self, dtype: typing.Type[DispatchT], handler: typing.Callable[[DispatchT], ResultT]):
        ...

    def unregister(self, dtype: typing.Type[DispatchT]):
        ...

    def __call__(self, obj: object) -> ResultT:
        ...


def _customdispatch(func: typing.Callable[[...], ResultT] = None, docstring=None) -> DispatchingFunction[ResultT]:
    """Custom alternative to functools.singledispatch.

    Get a class instance so that static type checkers understand the presence of *register* and *unregister* attributes.

    Define the class in the closure of a decorator so that we can make sure that the docstring of the returned callable
    can be reliably resolved.
    """
    if docstring is None:
        docstring = func.__doc__

    class CustomDispatcher(DispatchingFunction[ResultT]):
        __doc__ = docstring

        _dispatchers: typing.MutableMapping[typing.Type[DispatchT], typing.Callable[[DispatchT], ResultT]]

        def __init__(self):
            # We use WeakKeyDictionary because the keys are likely to be classes,
            # and we don't intend to extend the life of the type objects (which might be temporary).
            self._dispatchers = weakref.WeakKeyDictionary()

        def register(self, dtype, handler):
            # Note that we don't expect references to bound methods to extend the life of the type.
            # TODO: confirm this assumption in a unit test.
            if dtype in self._dispatchers:
                raise ProtocolError('Encodable type appears to be registered already.')
            self._dispatchers[dtype] = handler

        def unregister(self, dtype):
            # As long as we use a WeakKeyDictionary, explicit unregistration should not be necessary.
            del self._dispatchers[dtype]

        def __call__(self, obj) -> ResultT:
            # Currently, we iterate because we may be using abstract types for encoding.
            # If we find that we are using concrete types and/or we need more performance,
            # or if we just find that the list gets enormous, we can inspect the object first
            # to derive a dtype key that we can look up directly.
            # Warning: we should be careful not to let objects unexpectedly match multiple entries.
            for dtype, dispatch in self._dispatchers.items():
                if isinstance(obj, dtype):
                    return dispatch(obj)
            return func(obj)

    dispatch = CustomDispatcher()
    return dispatch


def _encode(obj) -> BaseEncodable:
    """Encode SCALE-MS objects as basic Python data that is easily serialized.

    Extend the JSONEncoder for representations in the SCALE-MS data model by
    passing to the *default* argument of ``json.dumps()``,
    but note that it will only be used for objects that JSONEncoder does not already
    resolve.
    """
    raise TypeError(f'No registered dispatching for {repr(obj)}')


# Note that using a decorator instead of clearly getting a class instance appears to suppress the
# static type checker's ability to inspect the method signatures.
encode: DispatchingFunction[BaseEncodable] = _customdispatch(_encode)

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
