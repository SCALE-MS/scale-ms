"""Specify the schema for encoded type descriptions."""
__all__ = ['EncodedObjectType',
           'EncodedShape',
           'EncodedTypeIdentifier']

import typing

NamedSchema = typing.Literal[
    'DataType',
    'CommandType',
    'Literal',
    'InputField',
    'DataField',
    'OutputField'
]


class EncodedSchema(typing.TypedDict):
    spec: typing.Literal['scalems.v0']
    name: NamedSchema


EncodedTypeIdentifier = typing.NewType('EncodedTypeIdentifier', typing.Tuple[str, ...])
"""A tuple representing the hierarchical namespace of the SCALE-MS object type."""

EncodedShapeElement = typing.NewType('EncodedShapeElement', int)
# We do not yet have support for key-word shape elements.
# EncodedShapeElement = typing.NewType('EncodedShapeElement', typing.Union[int, str])


# Note that JSON does not have a separate encoding for *tuple*
EncodedShape = typing.List[EncodedShapeElement]


class EncodedFieldType(typing.TypedDict):
    schema: EncodedSchema
    type: EncodedTypeIdentifier
    shape: EncodedShape


EncodedFields = typing.Mapping[str, EncodedFieldType]


class EncodedObjectType(typing.TypedDict):
    """SCALE-MS object type description encoded as a Python dictionary."""
    schema: EncodedSchema
    implementation: EncodedTypeIdentifier
    fields: EncodedFields
