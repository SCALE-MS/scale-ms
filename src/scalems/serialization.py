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

__all__ = ["BasicSerializable", "decode", "encode", "Shape"]

import abc
import collections.abc
import json
import logging
import os
import pathlib
import typing
import weakref

from scalems._types import BaseDecoded
from scalems._types import BaseEncodable
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.identifiers import EphemeralIdentifier
from scalems.identifiers import Identifier
from scalems.identifiers import TypeDataDescriptor
from scalems.identifiers import TypeIdentifier

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class Shape(tuple):
    """Describe the data shape of a SCALEMS object."""

    def __new__(cls, elements: typing.Iterable):
        return super().__new__(cls, elements)

    def __init__(self, elements: typing.Iterable):
        """Initial implementation requires a sequence of integers.

        Software requirements include symbolic elements, TBD.
        """
        try:
            es = tuple(e for e in elements)
        except TypeError as e:
            raise e
        if len(es) < 1 or any(not isinstance(e, int) for e in es):
            raise TypeError("Shape is a sequence of 1 or more integers.")


# It could make sense to split the codec for native-Python encoding from the
# (de)serialization code in the future...


class SchemaDict(typing.TypedDict):
    """Schema for the member that labels an object's schema.

    This is just a type hint for the moment. The specification can be strengthened
    in the core data model and module constants provided for the schema comprising
    the full specification.

    Notes:
        * Python 3.9 provides a "frozenmap"
        * Consider a namedtuple, dataclass, or similar and make dict interconversion secondary.
        * We should clarify object model policies such as the invariance/covariance/contravariance
          of members through subtyping.

    TODO: Allow equality check
    TODO: Actually integrate with object support metaprogramming in the package.
    """

    spec: str
    name: str


class SymbolicDimensionSize(typing.TypedDict):
    DimensionSize: str


ShapeElement = typing.Union[int, SymbolicDimensionSize]


class FieldDict(typing.TypedDict):
    """Members of the *fields* member of a ResourceType."""

    schema: SchemaDict
    type: typing.List[str]
    shape: typing.List[ShapeElement]


FieldsType = typing.Mapping[str, FieldDict]


class TypeDict(typing.TypedDict):
    """Express the expected contents of a dictionary-based type description."""

    schema: SchemaDict
    implementation: typing.List[str]
    fields: FieldsType


class Encoded(typing.Protocol):
    """An Encoded object and all of its nested data are BaseEncodable.

    E.g.

        typing.Mapping[str, typing.Union[
            'Encoded',
            typing.Sequence['Encoded'], # should this be a union?
            str,
            int,
            float,
            bool,
            type(None)]]
    """

    # TODO: How should we implement this?


DispatchT = typing.TypeVar("DispatchT")
ResultT = typing.TypeVar("ResultT")
S = typing.TypeVar("S")


class Serializable(abc.ABC):
    """Base class for serialization behaviors.

    Subclassing takes care of registering encoders and decoders at
    module import.

    Serializable types must support encoding to and decoding from
    a small set of basic Python types for serialization schemes,
    such as JSON.
    """

    @abc.abstractmethod
    def encode(self) -> typing.Union[dict, list, tuple, str, int, float, bool, None]:
        ...

    @classmethod
    @abc.abstractmethod
    def decode(cls: typing.Type[S], *args, **kwargs) -> S:
        ...

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Create a weakref and add to Encoder._encoders with a finalizer to remove from same.
        # Similarly add to the Decoder dispatcher...


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
    _dispatchers: typing.ClassVar[
        typing.MutableMapping[type, typing.Callable[[DispatchT], BaseEncodable]]
    ] = weakref.WeakKeyDictionary()

    @classmethod
    def register(cls, dtype: typing.Type[DispatchT], handler: typing.Callable[[DispatchT], BaseEncodable]):
        # Note that we don't expect references to bound methods to extend the life of the type.
        # TODO: confirm this assumption in a unit test.
        if not isinstance(dtype, type):
            raise TypeError("We use `isinstance(obj, dtype)` for dispatching, so *dtype* must be a `type` object.")
        if dtype in cls._dispatchers:
            raise ProtocolError(f"Encodable type {dtype} appears to be registered already.")
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
            if isinstance(obj, typing.cast(type, dtype)):
                return dispatch(obj)
        raise TypeError(f"No registered dispatching for {repr(obj)}")

    def __call__(self, obj) -> BaseEncodable:
        return self.encode(obj)


class UnboundObject(typing.Protocol):
    """A prototypical instance of a workflow item not bound to a workflow.

    Generally, SCALEMS objects are items in a managed workflow.
    """

    def shape(self) -> Shape:
        ...

    def dtype(self) -> TypeIdentifier:
        ...

    def encode(self) -> BaseEncodable:
        ...


class PythonDecoder:
    """Convert dictionary representations to SCALE-MS objects for registered types.

    Dictionaries are recognized as SCALE-MS object representations with a minimal heuristic.

    If the object (dict) contains a *'schema'* key, and the value
    is a dict, the *'spec'* member of the dict is retrieved. If the *'spec'* member exists and
    names a recognized schema specification, the object is dispatched according to the schema
    specification.

    Otherwise, if the object contains a *'type'* key, identifying a recognizable registered type,
    the object is dispatched to the decoder registered for that type.

    For more information, refer to the :doc:`serialization` and :doc:`datamodel` documentation.

    .. todo:: Consider specifying a package metadata resource group to allow packages to register
              additional schema through an idiomatic plugin system.
    Refs:
     * https://packaging.python.org/guides/creating-and-discovering-plugins/
     * https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html#dynamic-discovery-of-services-and-plugins
    """

    _dispatchers: typing.MutableMapping[TypeIdentifier, typing.Callable] = dict()

    # Depending on what the callables are, we may want a weakref.WeakValueDictionary() or we may not!

    @classmethod
    def register(cls, typeid: TypeIdentifier, handler: typing.Callable):
        # Normalize typeid
        typeid = TypeIdentifier.copy_from(typeid)
        if typeid in cls._dispatchers:
            raise ProtocolError("Type appears to be registered already.")
        cls._dispatchers[typeid] = handler

    @classmethod
    def unregister(cls, typeid: TypeIdentifier):
        del cls._dispatchers[typeid]

    @classmethod
    def get_decoder(cls, typeid) -> typing.Union[None, typing.Callable]:
        # Normalize the type identifier.
        try:
            identifier: typing.Optional[TypeIdentifier] = TypeIdentifier.copy_from(typeid)
            assert isinstance(identifier, TypeIdentifier)
            typename = identifier.name()
        except TypeError:
            try:
                typename = str(typeid)
            except TypeError:
                typename = repr(typeid)
            identifier = None
        # Use the (hashable) normalized form to look up a decoder for dispatching.
        if identifier is None or identifier not in cls._dispatchers:
            raise TypeError("No decoder registered for {}".format(typename))
        return cls._dispatchers[identifier]

    @classmethod
    def decode(cls, obj) -> typing.Union[UnboundObject, BaseDecoded]:  # noqa: C901
        """Create unbound SCALE-MS objects from their basic Python representations.

        We assume this is called in a bottom-up manner as a nested record is deserialized.

        Unrecognized objects are returned unaltered because they may be members
        of an enclosing object with appropriate dispatching.

        .. todo:: Consider where to register transcoders for compatible/virtual types.
                  E.g. Infer np.array(..., dtype=int) -> scalems.Integer
                  This is a small number of cases, since we can lean on the descriptors in the buffer protocol.
        """
        if not isinstance(obj, dict):
            # Probably don't have any special handling for such objects until we know what they are nested in.
            ...
        else:
            assert isinstance(obj, dict)
            if "schema" in obj:
                # We currently have very limited schema processing.
                try:
                    spec = obj["schema"]["spec"]
                except KeyError:
                    spec = None
                if not isinstance(spec, str) or spec != "scalems.v0":
                    # That's fine...
                    logger.info("Unrecognized *schema* when decoding object.")
                    return obj
                if "name" not in obj["schema"] or not isinstance(obj["schema"]["name"], str):
                    raise InternalError("Invalid schema.")
                else:
                    # schema = obj['schema']['name']
                    ...
                # Dispatch the object...
                ...
                raise MissingImplementationError(
                    "We do not yet support dynamic type registration through the work record."
                )

            if "type" in obj:
                # Dispatch the decoding according to the type.
                try:
                    dispatch = cls.get_decoder(obj["type"])
                except TypeError:
                    dispatch = BasicSerializable.decode
                if dispatch is not None:
                    return dispatch(obj)
        # Just return un-recognized objects unaltered.
        return obj

    def __call__(self, obj) -> typing.Union[UnboundObject, BaseDecoded]:
        return self.decode(obj)


encode = PythonEncoder()
decode = PythonDecoder()

# TODO: use stronger check for UID, or bytes-based objects.
encode.register(dtype=bytes, handler=bytes.hex)
encode.register(dtype=pathlib.Path, handler=os.fsdecode)
# TODO: Check that this dispatches correctly and update the type hinting.
# mypy gives "error: Only concrete class can be given where "Type[_PathLike[_AnyStr_co]]" is expected"
encode.register(dtype=os.PathLike, handler=os.fsdecode)


# Note that the low-level encoding/decoding is not necessarily symmetric because nested objects may be decoded
# according to the schema of a parent object.
# decode.register()


# A SCALE-MS "Serializable Type".
# TODO: use a Protocol or other constraint.
ST = typing.TypeVar("ST", bound="BasicSerializable")


class BasicSerializable(UnboundObject):
    __label: typing.Optional[str] = None
    __identity: Identifier
    _shape: Shape
    data: collections.abc.Container

    _data_encoder: typing.Callable
    _data_decoder: typing.Callable

    _dtype = TypeDataDescriptor(base_type=TypeIdentifier(("scalems", "BasicSerializable")))

    def dtype(self) -> TypeIdentifier:
        # Part of the decision of whether to use a property or a method
        # is whether we want to normalize on dtype as an instance or class characteristic.
        # Initially, we are using inheritance to get registration behavior through metaprogramming.
        # In other words, the real question may be how we want to handle registration.
        return self._dtype

    def __init__(self, data, *, dtype, shape=(1,), label=None, identity=None):
        if identity is None:
            # TODO: Calculate an appropriate identifier
            self.__identity = EphemeralIdentifier()
        else:
            # TODO: Validate identity
            self.__identity = identity
        self.__label = str(label)

        attrname = BasicSerializable._dtype.attr_name
        setattr(self, attrname, TypeIdentifier.copy_from(dtype))

        self._shape = Shape(shape)
        # TODO: validate data dtype and shape.
        # TODO: Ensure that we retain a reference to read-only data.
        # TODO: Allow a secondary localized / optimized / implementation-specific version of data.
        self.data = data

    def identity(self):
        return self.__identity

    def label(self):
        return str(self.__label)

    def shape(self):
        return Shape(self._shape)

    def encode(self) -> dict:
        representation = {
            "label": self.label(),
            "identity": str(self.identity()),
            "type": self.dtype().encode(),
            "shape": tuple(self.shape()),
            "data": self.data,  # TODO: use self._data_encoder()
        }
        return representation

    @classmethod
    def decode(cls: typing.Type[ST], encoded: dict) -> ST:
        if not isinstance(encoded, collections.abc.Mapping) or "type" not in encoded:
            raise TypeError("Expected a dictionary with a *type* specification for decoding.")
        dtype = TypeIdentifier.copy_from(encoded["type"])
        label = encoded.get("label", None)
        identity = encoded.get("identity")  # TODO: verify and use type schema to decode.
        shape = Shape(encoded["shape"])
        data = encoded["data"]  # TODO: use type schema / self._data_decoder to decode.
        logger.debug(f"Decoding {identity} as BasicSerializable.")
        return cls(label=label, identity=identity, dtype=dtype, shape=shape, data=data)

    def __init_subclass__(cls, **kwargs):
        assert cls is not BasicSerializable

        # Handle SCALE-MS Type registration.
        base = kwargs.pop("base_type", None)
        if base is not None:
            typeid = TypeIdentifier.copy_from(base)
        else:
            typeid = [str(cls.__module__)] + cls.__qualname__.split(".")
        registry = BasicSerializable._dtype.base
        if cls in registry and registry[cls] is not None:
            # This may be a customization or extension point in the future, but not today...
            raise ProtocolError("Subclassing BasicSerializable for a Type that is already registered.")
        BasicSerializable._dtype.base[cls] = typeid

        # Register encoder for all subclasses. Register the default encoder if not overridden.
        # Note: This does not allow us to retain the identity of *cls* for when we call the helpers.
        # We may require such information for encoder functions to know why they are being called.
        encoder = getattr(cls, "encode", BasicSerializable.encode)
        PythonEncoder.register(cls, encoder)

        # Optionally, register a new decoder.
        # If no decoder is provided, use the basic decoder.
        if hasattr(cls, "decode") and callable(cls.decode):
            _decoder = weakref.WeakMethod(cls.decode)

            # Note that we do not require that the decoded object is actually
            # an instance of cls.

            def _decode(encoded: dict):
                decoder = _decoder()
                if decoder is None:
                    raise ProtocolError("Decoding a type that has already been de-registered.")
                return decoder(encoded)

            PythonDecoder.register(cls._dtype, _decode)

        # TODO: Register optional instance initializer / input processor.
        # Allow instances to be created with something other than a single-argument
        # of the registered Input type.

        # TODO: Register/generate UI helper.
        # From the user's perspective, an importable module function interacts
        # with the WorkflowManager to add workflow items and return a handle.
        # Do we want to somehow generate an entry-point command

        # TODO: Register result dispatcher(s).
        # An AbstractDataSource must register a dispatcher to an implementation
        # that produces a ConcreteDataSource that provides the registered Result type.
        # A ConcreteDataSource must provide support for checksum calculation and verification.
        # Optionally, ConcreteDataSource may provide facilities to convert to/from
        # native Python objects or other types (such as .npz files).

        # Proceed dispatching along the MRO, per documented Python data model.
        super().__init_subclass__(**kwargs)


def compact_json(obj) -> str:
    """Produce the compact JSON string for the encodable object."""
    # Use the extensible Encoder from the serialization module, but apply some output formatting.
    string = json.dumps(obj, default=encode, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return string


class JsonObjectPairsDispatcher:
    """Decode key/value pairs from JSON objects into SCALE-MS objects.

    Provides a place to register different type handlers.

    Each JSON *object* deserialized by the JSON Decoder is passed as a sequence
    of (key, value) pairs. The result is returned instead of the usual *dict(pairs)*.

    We don't have interaction with state or nesting, so we may have to examine the
    values for Python objects that have already been decoded to see if additional
    processing is necessary once the context of the key/value pair is known.
    """

    def __call__(self, key, value):
        ...


# def object_pair_decoder(context, object_pairs: typing.Iterable[typing.Tuple[str, typing.Any]])\
#         -> typing.Iterable[ItemView]:
#     """Decode named objects, updating the managed workflow as appropriate.
#
#     For object pairs representing complete workflow items, get a handle to a managed workflow item.
#     If the key is already managed, update the the managed item or raise an error if the managed item
#     is not consistent with the received item.
#
#     Note that responsibilities for validating work graphs, data flow, and compatibility are delegated to the
#     WorkflowManager and the registered data and command types. It does not make sense to call this function without
#     a proper WorkflowManager. For low-level testing or other use cases, consider directly using PythonDecoder.
#
#     To extend json.load() or json.loads(), use functools.partial to bind a workflow context, and pass the
#     partially bound function as the *object_pairs_hook* argument to the json deserializer.
#     """
#     # We would generally want to deserialize directly into a WorkflowManager. We could write this as a free function
#     # and optionally bind it as a method. We could also make it a singledispatch function or a singledispatchmethod.
#     # These are probably not mutually exclusive.
#     for key, value in object_pairs:
#         # dispatch decoding for value
#         # if is_workflowitem(decoded):
#         #    identity = validate_id(key, decoded)
#         #    record = {identity: decoded}
#         item_view = context.add_item(record)
#         yield item_view

Key = typing.Union[str, int, slice]


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
