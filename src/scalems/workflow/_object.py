"""Basic Python support for the SCALE-MS workflow object model."""
__all__ = [
    'Data',
    'Object',
]

import abc
import typing

from scalems._types import Shape
from scalems.identifiers import Identifier
from scalems.identifiers import ResourceIdentifier
from scalems.identifiers import TypeIdentifier


# ``MetaField[_InfoT]`` isn't useful without some additional metaprogramming,
# which seems unnecessary at this point.
#
# _InfoT = typing.TypeVar('_InfoT')
# """Meta-information base type.
#
# Probably either a Mapping[str, str], a Tuple, or (possibly) a str.
# """
#
#
# class MetaField(abc.ABC, typing.Generic[_InfoT]):
#     """Fields that are important to an object's representation, but not part of the
#     instance data.
#     """


class Schema(typing.TypedDict):
    """Meta-Field defining the schema for the object structure.

    The following are equivalent::

        class MyDataType:
            schema = scalems.Schema('DataType')
            ...

        @scalems.datatype
        class MyDataType:
            ...

        @scalems.type(schema='DataType')
        class MyDataType:
            ...
    """
    spec: str  # Example: "scalems.v0"
    name: str


class Implementation(typing.Tuple[str, ...]):
    """Meta-Field giving the importable implementation for the Type.

    Example::

        class MyDataType:
            schema = scalems.Schema('DataType')
            implementation = scalems.Implementation(('mymodule', 'MyDataType'))
            ...

    Note that this field is expected to be generated for the ObjectType instance
    when a scalems-annotated implementation class is imported and automatically
    registered. This example is just for illustration.
    """


_T = typing.TypeVar('_T')


class Literal(typing.Protocol[_T]):
    """SCALE-MS schema of a native literal value."""
    spec: typing.ClassVar = 'scalems.v0'
    implementation: typing.ClassVar[typing.Callable[[typing.Any], _T]]


class IntegerLiteral(abc.ABC, Literal[int]):
    implementation = int
    value: int


# TODO: Follow up on how to handle the native types.
# Note: some static type checkers don't recognize the `register` method added by
# abc.ABC's metaclass.
# IntegerLiteral.register(int)


class FieldType(abc.ABC):
    """Describe a field in a workflow object type.

    There is no general field type. Field types are defined with the SCALE-MS
    specification. Concrete field types currently include InputField,
    OutputField, and DataField.

    TBD: If these classes are to serve any purpose beyond annotation, it is not yet clear
    what interface they should provide.

    Note: Any knowable SCALE-MS data type should be allowable as a field type.
    Additionally, several native Python types must be allowable. Furthermore,
    it is important in several use cases to allow opaque references or even opaque
    embedded objects that the workflow manager cannot interpret and must defer to an
    external module. These latter cases may have unique registration schemes,
    and require further design.
    """
    ...


class ObjectType(typing.Protocol):
    """Describe a workflow object type."""
    schema: Schema
    implementation: Implementation
    fields: typing.Mapping[str, FieldType]


class Object(typing.Protocol):
    """Common Python interface for workflow items.

    Workflow items must be able to report their *identity*, *type*, and *shape*,
    as well as their (optional) user-provided label.

    Proxy attributes (Futures) for data fields are according to the specific object type.
    """

    def label(self) -> typing.Optional[str]:
        raise NotImplementedError

    def identity(self) -> Identifier:
        raise NotImplementedError

    def type(self) -> TypeIdentifier:
        raise NotImplementedError

    def shape(self) -> Shape:
        raise NotImplementedError


class CommandType(ObjectType):
    ...


class Command(Object):
    ...


class DataType(ObjectType):
    ...


class Data(Object):
    """Concrete data available in the context of the workflow.

    Actual stored data represented by a Data instance is not necessarily contained
    within or owned by the instance, but is guaranteed to be locally available to the
    WorkflowManager with a unique, verifiable, and reproducible identity.

    The method of fingerprinting and the data schema are details of the DataType
    implementation.

    TODO: Do we need a base class? Or a common class for composed instances?
    """

    def label(self) -> typing.Optional[str]:
        pass

    def identity(self) -> ResourceIdentifier:
        pass

    def type(self) -> TypeIdentifier:
        pass

    def shape(self) -> Shape:
        pass
