"""Test the scalems package data model helpers and metaprogramming utilities.
"""
import abc
import collections.abc
import json
import logging
import os
import pathlib
import typing
import uuid
import weakref

import pytest
from scalems.core.exceptions import InternalError
from scalems.core.exceptions import MissingImplementationError
from scalems.core.exceptions import ProtocolError

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

record = """{
    "version"= "scalems_workflow_1",
    "types"= {
        "scalems.SubprocessInput" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessInput"],
            "fields" = {
                "argv" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "String"],
                    "shape"= ["constraints.OneOrMore"]
                },
                "inputs" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "outputs" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "stdin" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "environment" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                },
                "resources" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                }
            }            
        },
        "scalems.SubprocessResult" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessResult"],
            "fields" = {
                "exitcode" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Integer"],
                    "shape"= [1]
                },
                "stdout" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "stderr" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "File"],
                    "shape"= [1]
                },
                "file" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "Mapping"],
                    "shape"= [1]
                }
            }
        },
        "scalems.Subprocess" = {
            "schema" = {
                "spec" = "scalems.v0",
                "name" = "DataType"
            },
            "implementation" = ["scalems", "subprocess", "SubprocessResult"],
            "fields" = {
                "input" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "SubprocessInput"],
                    "shape"= [1]
                },
                "result" = {
                    "schema" = {
                        "spec" = "scalems.v0",
                        "name" = "DataField"
                    },
                    "type"= ["scalems", "SubprocessResult"],
                    "shape"= [1]
                }
            }
        },
    },
    "items"= [
        {
            "label"= "input_files",
            "identity"= "832df1a2-1f0b-4024-a4ab-4160717b8a8c",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"-i"= ["infile"]}
        },
        {
            "label"= "output_files",
            "identity"= "d19e2734-a23c-42de-88a0-3287d7ca71ac",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"-o"= ["outfile"]}
        },
        {
            "label"= "resource_spec",
            "identity"= "34dfc648-27b3-47db-b6a8-a10c9ae58f09",
            "type"= ["scalems", "Mapping"],
            "shape"= [1],
            "data"= {"ncpus"= 8, "launch_method"= ["exec"]}
        },
        {
            "label"= "subprocess_input",
            "identity"= "26c86a70-b407-471c-85ed-c1ebfa52f592",
            "type"= ["scalems", "SubprocessInput"],
            "shape"= [1],
            "data"= {
                "args"= ["myprogram", "--num_threads", "8"],
                "inputs"= "832df1a2-1f0b-4024-a4ab-4160717b8a8c",
                "outputs"= "d19e2734-a23c-42de-88a0-3287d7ca71ac",
                "stdin"= null,
                "environment" = [{}],
                "resources" = "34dfc648-27b3-47db-b6a8-a10c9ae58f09"
            },
        },
        {
            "label"= "command",
            "identity"= "199f214c-98f5-4fdd-929e-42685f8c00d2",
            "type"= ["scalems", "Subprocess"],
            "shape"= [1],
            "data"= {
                "input"= "26c86a70-b407-471c-85ed-c1ebfa52f592",
                "result"= "199f214c-98f5-4fdd-929e-42685f8c00d2"
            }
        }

    ]
}
"""

json_base_encodable_types: typing.Tuple[type, ...] = (dict, list, tuple, str, int, float, bool, type(None))
json_base_decoded_types: typing.Tuple[type, ...] = (dict, list, str, int, float, bool, type(None))

BaseEncodable = typing.Union[dict, list, tuple, str, int, float, bool, None]
BaseDecoded = typing.Union[dict, list, str, int, float, bool, None]

##############################
# TODO: Move to fingerprint.py
# TODO: Use a URL to the schema or specification.
NAMESPACE_SCALEMS: uuid.UUID = uuid.uuid5(uuid.NAMESPACE_DNS, 'scalems.org')


FingerprintHash = typing.NewType('FingerprintHash', bytes)
"""The fingerprint hash is a 32-byte sequence containing a SHA256 digest."""
##############################


#############################
# TODO: move (to _detail.py?)
@typing.runtime_checkable
class Identifier(typing.Hashable, typing.Protocol):
    """SCALE-MS object identifiers support this protocol.

    Identifiers may be implemented in terms of a hashing scheme, RFC 4122 UUID,
    or other encoding appropriate for the scope of claimed uniqueness and
    reusability (cacheable).

    Namespace UUIDs are appropriate for strongly specified names, such as operation implementation identifiers.
    The 48 bits of data are sufficient to identify graph nodes at session scope.
    At workflow scope, we need additional semantics about what should persist or not.

    Concrete data warrants a 128-bit or 256-bit checksum.


    """
    scope: str
    """Scope in which the Identifier is effective and unique."""
    # TODO: Use an enum that is part of the API specification.

    reproducible: bool
    """Whether results will have the same identity if re-executed, such as due to missing cache."""

    concrete: bool
    """Is this a concrete object or something more abstract?"""

    @abc.abstractmethod
    def bytes(self) -> typing.SupportsBytes:
        """The core interface provided by Identifiers is a consistent bytes representation of their identity.

        Note that the identity (and the value returned by self.bytes()) must be immutable for
        the life of the object.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __str__(self) -> str:
        """Represent the identifier in a form suitable for serialized records.

        All Identifier subclasses must explicitly define the string representation,
        but may ``return super().__str__(self)`` for a suitable default
        (a hexadecimal encoding of the core data).

        By default, the string representation is the basis for the stub used
        for filesystem objects. To change this, override the __fspath__() method.
        """
        return hex(self)

    def encode(self) -> BaseEncodable:
        """Get a canonical encoding of the identifier as a native Python object.

        This is the method that will be used to produce serialized workflow records.

        By default, the string representation (self.__str__()) is used.
        Subclasses may override, as long as suitable decoding is possible and provided.
        """
        return str(self)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.bytes() == self.bytes()

    def __hash__(self) -> int:
        # Note that the result of the `hash()` built-in is truncated to
        # the size of a `Py_ssize_t`, but two objects that compare equal must
        # return the same value for `__hash__()`, so we return the full value.
        return self.__index__()

    def __fspath__(self) -> str:
        """Get a representation suitable for naming a filesystem object."""
        path = os.fsencode(str(self))
        return str(path)

    def __bytes__(self) -> bytes:
        """Get the network ordered byte sequence for the raw identifier."""
        return bytes(self.bytes())

    def __index__(self) -> int:
        """Support integer conversions."""
        return int.from_bytes(self.bytes(), 'big')


class EphemeralIdentifier(Identifier):
    """Process-scoped UUID based identifier.

    Not reproducible. Useful for tracking objects within a single process scope.
    """
    def __init__(self, node=None, clock_seq=None):
        self._data = uuid.uuid1(node, clock_seq)

    def bytes(self):
        return self._data.bytes

    def __str__(self) -> str:
        return str(self._data)


class NamedIdentifier(Identifier):
    """A name with strong identity semantics, represented with a UUID."""
    # TODO: facility to annotate scope
    # TODO: facility to annotate reproducibility
    # TODO: facility to indicate whether this is a reference to concrete data or not.
    def __init__(self, nested_name: typing.Sequence[str]):
        try:
            if isinstance(nested_name, (str, bytes)):
                raise TypeError('Wrong kind of iterable.')
            self._name_tuple = tuple(str(part) for part in nested_name)
        except TypeError as e:
            raise TypeError(f'Could not construct {self.__class__.__name__} from {repr(nested_name)}')
        else:
            self._data = uuid.uuid5(NAMESPACE_SCALEMS, '.'.join(self._name_tuple))
        # TODO: The instance should track a context in which the uuid can be resolved.

    def bytes(self):
        return self._data.bytes

    def __str__(self) -> str:
        return str(self._data)

    def encode(self) -> BaseEncodable:
        return self._name_tuple


class ResourceIdentifier(Identifier):
    # TODO: facility to annotate scope
    # TODO: facility to annotate reproducibility
    # TODO: facility to indicate whether this is a reference to concrete data or not.
    def __init__(self, fingerprint: bytes):
        self._data = bytes(fingerprint)
        # Expecting a 256-bit SHA256 hash digest
        if len(self._data) != 32:
            raise InternalError(f'Expected a 256-bit hash digest. Got {repr(fingerprint)}')

    def bytes(self):
        return bytes(self._data)


class TypeIdentifier(NamedIdentifier):
    def name(self):
        return '.'.join(self._name_tuple)

    def scoped_name(self):
        return self._name_tuple

    @classmethod
    def copy_from(cls, typeid) -> 'TypeIdentifier':
        """Create a new TypeIdentifier instance describing the same type as the source.

        .. todo:: We need a generic way to determine the (registered) virtual type of an object, but that doesn't belong here.
        """
        if isinstance(typeid, NamedIdentifier):
            return cls(typeid._name_tuple)
        if isinstance(typeid, (list, tuple)):
            return cls(typeid)
        if isinstance(typeid, type):
            # Consider disallowing TypeIdentifiers for non-importable types.
            # (Requires testing and enforcement.)
            # Consider allowing class objects to self-report their type.
            if typeid.__module__ is not None:
                fully_qualified_name = '.'.join((typeid.__module__, typeid.__qualname__))
            else:
                fully_qualified_name = str(typeid.__qualname__)
            return cls.copy_from(fully_qualified_name)
        if isinstance(typeid, str):
            # TODO: First check if the string is a UUID or other reference form for a registered type.
            return cls.copy_from(tuple(typeid.split('.')))
        # TODO: Is there a dictionary form that we should allow?
#############################

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
            raise TypeError('Shape is a sequence of 1 or more integers.')


DispatchT = typing.TypeVar('DispatchT')


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


# UnboundObject = typing.NewType('UnboundObject', object)
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
    """
    # TODO: Consider specifying a package metadata resource group to allow packages to register
    #       additional schema through an idiomatic plugin system.
    # Refs:
    #  * https://packaging.python.org/guides/creating-and-discovering-plugins/
    #  * https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html#dynamic-discovery-of-services-and-plugins

    _dispatchers: typing.MutableMapping[
            TypeIdentifier,
            typing.Callable] = dict()

    @classmethod
    def register(cls, typeid: TypeIdentifier, handler: typing.Callable):
        # Normalize typeid
        typeid = TypeIdentifier.copy_from(typeid)
        if typeid in cls._dispatchers:
            raise ProtocolError('Type appears to be registered already.')
        cls._dispatchers[typeid] = handler

    @classmethod
    def unregister(cls, typeid: TypeIdentifier):
        del cls._dispatchers[typeid]

    @classmethod
    def get_decoder(cls, typeid) -> typing.Union[None, typing.Callable]:
        # Normalize the type identifier.
        try:
            identifier = TypeIdentifier.copy_from(typeid)
            typename = identifier.name()
        except TypeError:
            try:
                typename = str(typeid)
            except TypeError:
                typename = repr(typeid)
            identifier = None
        # Use the (hashable) normalized form to look up a decoder for dispatching.
        if identifier is None or identifier not in cls._dispatchers:
            raise TypeError('No decoder registered for {}'.format(typename))
        return cls._dispatchers[identifier]

    @classmethod
    def decode(cls, obj) -> typing.Union[UnboundObject, BaseDecoded]:
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
            if 'schema' in obj:
                # We currently have very limited schema processing.
                try:
                    spec = obj['schema']['spec']
                except KeyError:
                    spec = None
                if not isinstance(spec, str) or spec != 'scalems.v0':
                    # That's fine...
                    logger.info('Unrecognized *schema* when decoding object.')
                    return obj
                if 'name' not in obj['schema'] or not isinstance(obj['schema']['name'], str):
                    raise InternalError('Invalid schema.')
                else:
                    schema = obj['schema']['name']
                # Dispatch the object...
                ...
                raise MissingImplementationError(
                    'We do not yet support dynamic type registration through the work record.')

            if 'type' in obj:
                # Dispatch the decoding according to the type.
                try:
                    dispatch = cls.get_decoder(obj['type'])
                except TypeError:
                    dispatch = BasicSerializable.decode
                if dispatch is not None:
                    return dispatch(obj)
        # Just return un-recognized objects unaltered.
        return obj

    def __call__(self, obj) -> UnboundObject:
        return self.decode(obj)


# A SCALE-MS type.
# TODO: use a Protocol or other constraint.
ST = typing.TypeVar('ST')


class BasicSerializable:
    __label: typing.Optional[str] = None
    __identity: Identifier
    _dtype: TypeIdentifier
    _shape: Shape
    data: collections.abc.Container

    _data_encoder: typing.Callable
    _data_decoder: typing.Callable

    def __init__(self, data, *, dtype, shape=(1,), label=None, identity=None):
        if identity is None:
            # TODO: Calculate an appropriate identifier
            self.__identity = EphemeralIdentifier()
        else:
            # TODO: Validate identity
            self.__identity = identity
        self.__label = str(label)
        self._dtype = TypeIdentifier.copy_from(dtype)
        self._shape = Shape(shape)
        # TODO: validate data dtype and shape.
        # TODO: Ensure data is read-only.
        # TODO: Allow a secondary localized / optimized / implementation-specific version of data.
        self.data = data

    def identity(self):
        return self.__identity

    def label(self):
        return str(self.__label)

    def dtype(self):
        return TypeIdentifier.copy_from(self._dtype)

    def shape(self):
        return Shape(self._shape)

    def encode(self) -> dict:
        representation = {
            'label': self.label(),
            'identity': str(self.identity()),
            'type': self.dtype().encode(),
            'shape': tuple(self.shape()),
            'data': self.data  # TODO: use self._data_encoder()
        }
        return representation

    @classmethod
    def decode(cls: typing.Type[ST], encoded: dict) -> ST:
        if not isinstance(encoded, collections.abc.Mapping) or not 'type' in encoded:
            raise TypeError('Expected a dictionary with a *type* specification for decoding.')
        dtype = TypeIdentifier.copy_from(encoded['type'])
        label = encoded.get('label', None)
        identity = encoded.get('identity')  # TODO: verify and use type schema to decode.
        shape = Shape(encoded['shape'])
        data = encoded['data']  # TODO: use type schema / self._data_decoder to decode.
        logger.debug('Decoding {identity} as BasicSerializable.')
        return cls(label=label,
                   identity=identity,
                   dtype=dtype,
                   shape=shape,
                   data=data
                   )

    def __init_subclass__(cls, **kwargs):
        dtype = getattr(cls, 'dtype', None)
        if dtype is not None:
            encoder = getattr(cls, 'encode', None)
            if encoder is None:
                encoder = BasicSerializable.encode
            PythonEncoder.register(dtype, encoder)
            decoder = getattr(cls, 'decode', None)
            if decoder is None:
                decoder = BasicSerializable.decode
            PythonDecoder.register(dtype, decoder)
        super().__init_subclass__(**kwargs)


encode = PythonEncoder()
decode = PythonDecoder()

# TODO: use stronger check for UID, or bytes-based objects.
encode.register(dtype=bytes, handler=bytes.hex)
encode.register(dtype=os.PathLike, handler=os.fsdecode)

# Note that the low-level encoding/decoding is not necessarily symmetric because nested objects may be decoded
# according to the schema of a parent object.
# decode.register()

def test_shape():
    shape = Shape((1,))
    assert isinstance(shape, tuple)
    assert len(shape) == 1
    assert shape == (1,)

    shape = Shape((3,3))
    assert isinstance(shape, tuple)
    assert len(shape) == 2
    assert shape == (3,3)

    shape = Shape(Shape((1,)))
    assert isinstance(shape, tuple)
    assert len(shape) == 1
    assert shape == (1,)
    assert shape == Shape((1,))

    with pytest.raises(TypeError):
        Shape(1)


def test_encoding_str():
    """Confirm that bare strings are encoded and decoded as expected.

    Note: we may choose not to support bare strings through our serialization module.
    """
    string = 'asdf'
    serialized = json.dumps(string, default=encode)
    round_trip = json.loads(serialized)
    assert string == round_trip


def test_encoding_scalars():
    """Confirm that scalar data is encoded and decoded as expected.

    Note: We may choose not to support certain forms of data for the full round trip, particularly bare scalars.
    """
    scalar = 42
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip

    scalar = None
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip

    scalar = True
    serialized = json.dumps(scalar, default=encode)
    round_trip = json.loads(serialized)
    assert scalar == round_trip


def test_encoding_int():
    """Confirm that integer data is encoded and decoded as expected.

    Note: We may choose not to support certain forms of integer data for the full round trip.
    """
    series = [1, 1, 2, 3, 5]
    length = len(series)
    shape = (length,)

    # Test bare native int list.
    serialized = json.dumps(series, default=encode)
    round_trip = json.loads(serialized)
    assert all([a == b for a, b in zip(series, round_trip)])

    # We expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')

    array = [[1, 1], [2, 1], [8, 9]]
    shape = (3, 2)
    serialized = json.dumps(array, default=encode)
    round_trip = json.loads(serialized)
    for rowA, rowB in zip(array, round_trip):
        assert all([a == b for a, b in zip(rowA, rowB)])
    # We expect structured data to be automatically translated to SCALE-MS representation.
    # assert round_trip.shape() == shape
    # assert round_trip.dtype() == ('scalems', 'Integer')


def test_encoding_bytes():
    length=8
    data = (42).to_bytes(length=length, byteorder='big')
    serialized = json.dumps(data, default=encode)
    round_trip = json.loads(serialized)
    # TODO: decoder
    assert round_trip == data.hex()
    # assert data == bytes(round_trip)


def test_encoding_fileobject():
    import tempfile
    with tempfile.NamedTemporaryFile() as fh:
        filename = fh.name
        assert os.path.exists(filename)
        serialized = json.dumps(filename, default=encode)
        round_trip = json.loads(serialized)
        # TODO: decoder
        assert os.path.exists(round_trip)
        # assert round_trip.dtype() == ('scalems', 'File')
        # assert round_trip.shape() == (1,)
        assert pathlib.Path(filename) == pathlib.Path(round_trip)


def test_basic_decoding():
    # Let the basic encoder/decoder handle things that look like SCALE-MS objects.
    encoded = {
        'label': None,
        'identity': uuid.uuid4().hex,
        'type': ['test', 'Spam'],
        'shape': [1],
        'data': ['spam', 'eggs', 'spam', 'spam']
    }
    instance = decode(encoded)
    assert instance.shape() == Shape((1,))
    assert instance.dtype() == TypeIdentifier(('test', 'Spam'))

    # Test basic encoding, too.
    assert tuple(instance.encode()['data']) == tuple(encoded['data'])
    assert instance.encode() == decode(instance.encode()).encode()
    # TODO: Check non-trivial shape.


def test_encoder_registration():
    # Test low-level registration details for object representation round trip.
    class SpamInstance(BasicSerializable):
        def encode(self):
            representation = {
                'label': self.label,
                'identity': self.identity,
                'type': ('test', 'Spam'),
                'shape': self.shape(),
                'data': self._data  # TODO: register data encoder.
            }
            return representation

        @classmethod
        def decode(cls: typing.Type[ST], encoded: dict) -> ST:
            ...


    # Test framework for type creation and automatic registration.


def test_serialization():
    ...


def test_deserialization():
    # Check for valid JSON
    serialized = json.dumps(record, default=encode)
    assert json.loads(serialized)

    ...


def test_fingerprint():
    ...


def test_object_model():
    """Check that the decorators and annotation types work right.

    Class definitions use decorators and members of special types to generate
    definitions that support the SCALE-MS object model.

    Independently from any static type checking support, this test confirms that
    the utilities actually produce the objects and relationships we expect.
    """
