"""Protocols and typing tools for identifying SCALE-MS objects and interfaces.

TODO: Publish some schema to scalems.org for easy reference and verifiable UUID hashing.
"""

from __future__ import annotations

__all__ = [
    "NAMESPACE_SCALEMS",
    "FingerprintHash",
    "Identifier",
    "NamedIdentifier",
    "ResourceIdentifier",
    "TypeIdentifier",
    "OperationIdentifier",
    "TypeDataDescriptor",
    "EphemeralIdentifier",
]

import abc
import builtins
import logging
import os
import typing
import uuid
import weakref

from scalems._types import BaseEncodable
from scalems.exceptions import InternalError
from scalems.exceptions import ProtocolError

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

# TODO: Use a URL to the schema or specification.
NAMESPACE_SCALEMS: uuid.UUID = uuid.uuid5(uuid.NAMESPACE_DNS, "scalems.org")

FingerprintHash = typing.NewType("FingerprintHash", bytes)
"""The fingerprint hash is a 32-byte sequence containing a SHA256 digest."""


@typing.runtime_checkable
class Identifier(typing.Hashable, typing.Protocol):
    """SCALE-MS object identifiers support this protocol.

    Identifiers may be implemented in terms of a hashing scheme, RFC 4122 UUID,
    or other encoding appropriate for the scope of claimed uniqueness and
    reusability (cacheable).

    Namespace UUIDs are appropriate for strongly specified names, such as operation
    implementation identifiers. The 48 bits of data are sufficient to identify graph
    nodes at session scope. At workflow scope, we need additional semantics about what
    should persist or not.

    Concrete data warrants a 128-bit or 256-bit checksum.

    Design notes:
        Several qualifications may be necessary regarding the use of an identifier,
        which may warrant annotation accessible through the Identifier instance.
        Additional proposed data members include:

        * scope: Scope in which the Identifier is effective and unique.
        * reproducible: Whether results will have the same identity if re-executed.
          Relates to behaviors in situations such as missing cache data.
        * concrete: Is this a concrete object or something more abstract?

    """

    concrete: bool
    """Is this a concrete object or something more abstract?"""

    @abc.abstractmethod
    def bytes(self) -> bytes:
        """A consistent bytes representation of the identity.

        The core interface provided by Identifiers.

        Note that the identity (and the value returned by self.bytes()) must be immutable
        for the life of the object.
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
        return self.bytes().hex()

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
        """Get a representation suitable for naming a filesystem object.

        Support the `os.PathLike` protocol for `os.fspath`.
        """
        path = os.fsencode(str(self))
        return str(path)

    def __bytes__(self) -> builtins.bytes:
        """Get the network ordered byte sequence for the raw identifier."""
        return self.bytes()

    def __index__(self) -> int:
        """Support integer conversions, including the `hex()` builtin function."""
        return int.from_bytes(self.bytes(), "big")


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
                raise TypeError("Wrong kind of iterable.")
            self._name_tuple = tuple(str(part) for part in nested_name)
        except TypeError as e:
            raise TypeError(f"Could not construct {self.__class__.__name__} from {repr(nested_name)}") from e
        else:
            self._data = uuid.uuid5(NAMESPACE_SCALEMS, ".".join(self._name_tuple))
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
            raise InternalError(f"Expected a 256-bit hash digest. Got {repr(fingerprint)}")

    def bytes(self):
        return self._data

    def __str__(self) -> str:
        return self._data.hex()


class TypeIdentifier(NamedIdentifier):
    def name(self):
        return ".".join(self._name_tuple)

    def scoped_name(self) -> typing.Tuple[str, ...]:
        return self._name_tuple

    @classmethod
    def copy_from(cls, typeid) -> "TypeIdentifier":
        """Create a new TypeIdentifier instance describing the same type as the source.

        .. todo:: We need a generic way to determine the (registered) virtual type
                  of an object, but that doesn't belong here.
        """
        if isinstance(typeid, NamedIdentifier):
            # Copy from a compatible object.
            return cls(typeid._name_tuple)
        elif isinstance(typeid, (list, tuple)):
            # Create from the usual initialization parameter type.
            return cls(typeid)
        elif isinstance(typeid, type):
            # Try to generate an identifier based on a defined class.
            #
            # Consider disallowing TypeIdentifiers for non-importable types.
            # (Requires testing and enforcement.)
            # Consider allowing class objects to self-report their type.
            if typeid.__module__ is not None:
                fully_qualified_name = ".".join((typeid.__module__, typeid.__qualname__))
            else:
                fully_qualified_name = str(typeid.__qualname__)
            return cls.copy_from(fully_qualified_name)
        elif isinstance(typeid, str):
            # Conveniently try to convert string representations back into
            # the namespace sequence representation.
            # TODO: First check if the string is a UUID or other reference form
            #  for a registered type.
            return cls.copy_from(tuple(typeid.split(".")))
        # TODO: Is there a dictionary form that we should allow?
        else:
            raise TypeError(f"Cannot create a TypeIdentifier from {repr(typeid)}")


class TypeDataDescriptor:
    """Implement the *dtype* attribute.

    The TypeDataDescriptor object is instantiated to implement the
    BasicSerializable.base_type dynamic attribute.

    Attributes:
        name: Name of the attribute provided by the data descriptor.
        base: TypeIdentifier associated with the Python class.

    *name* can be provided at initialization, but is overridden during class
    definition when TypeDataDescriptor is used in the usual way (as a data descriptor
    instantiated during class definition).

    At least for now, *name* is required to be ``_dtype``.

    *attr_name* is derived from *name* at access time. For now, it is always
    ``__dtype``.

    Instances of the Python class may have their own *dtype*. For the SCALE-MS
    data model, TypeIdentifier is an instance attribute rather than a class attribute.
    If an instance did not set ``self.__dtype`` at initialization, the descriptor
    returns *base* for the instance's class.

    *base* is the (default) SCALEMS TypeIdentifier for the class using the descriptor.
    For a class using the data descriptor, *base* is inferred from the class
    __module__ and __qualname__ attributes, if not provided through the class definition.

    A single data descriptor instance is used for a class hierarchy to encapsulate
    the meta-programming for UnboundObject classes without invoking Python metaclass
    arcana (so far). At module import, a TypeDataDescriptor is instantiated for
    BasicSerializable._dtype. The data descriptor instance keeps a weakref.WeakKeyDict
    mapping type objects (classes) to the TypeDataDescriptor details for classes
    other than BasicSerializable. (BasicSerializable._dtype always produces
    ``TypeIdentifier(('scalems', 'BasicSerializable'))``.)
    The mapping is updated whenever BasicSerializable is subclassed.
    """

    base: typing.MutableMapping[type, TypeIdentifier]
    _original_owner_type: typing.Optional[TypeIdentifier]

    @property
    def attr_name(self):
        """Name of the instance data member used by this descriptor for storage."""
        return "_owner" + self.name

    def __init__(self, name: str = None, base_type: TypeIdentifier = None):
        # Note that the descriptor instance is not fully initialized until it is
        # further processed during the creation of the owning class.
        self.name = name
        if base_type is not None:
            self._original_owner_type = TypeIdentifier.copy_from(base_type)
        else:
            self._original_owner_type = None
        self.base = weakref.WeakKeyDictionary()

    def __set_name__(self, owner, name):
        # Called by type.__new__ during class creation to allow customization.
        # Let's start with strict naming requirements for early implementations,
        # and explicitly forbid multiple instances of this data descriptor implementation
        # in the same class.
        # Note that __set_name__ is only called at most once, by type.__new__
        # for a class definition in which the descriptor is instantiated.
        # In other words, __set_name__ is called for the base class, only, and
        # __init_subclass__ is called for derived classes, only.
        if name != "_dtype":
            raise ProtocolError("TypeDataDescriptor has a strict naming protocol. Only use for a `_dtype` attribute.")
        self.name = name
        if hasattr(owner, self.attr_name):
            raise ProtocolError(
                f"No storage for data descriptor. {repr(owner)} already has an attribute named {self.attr_name}."
            )

        assert owner not in self.base
        assert len(self.base) == 0
        logger.debug(f"Initializing base class {owner} ownership of TypeDataDescriptor.")
        self._original_owner = weakref.ref(owner)
        if self._original_owner_type is None:
            self._original_owner_type = TypeIdentifier.copy_from(
                [str(owner.__module__)] + owner.__qualname__.split(".")
            )
        self.base[owner] = TypeIdentifier.copy_from(self._original_owner_type)

    def __get__(self, instance, owner):
        # Note that instance==None when called through the *owner* (as a class attribute).
        if instance is None:
            if owner is self._original_owner():
                return self
            return self.base[owner]
        return getattr(instance, self.attr_name, self.base[owner])


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
        return ".".join(self)
