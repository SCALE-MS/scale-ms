"""Specify the schema for encoded workflow records.

The `scalems` Python package specifies a data structure for holding a workflow record (in
terms of native Python objects) that is unambiguously serializable and deserializable
using the `json` package. We avoid strongly specifying JSON as the actual serialization
format.
"""
__all__ = ['EncodedRecordDict']

import typing

from . import _object
from . import _type

VersionLiteral = typing.Literal['scalems_workflow_1']
"""String values for the *version* field.

A dictionary with ``version: 'scalems_workflow_1'`` is assumed to conform to
the schema defined in the current module.
"""

TypesRecord = typing.Mapping[str, _type.EncodedObjectType]
"""Schema for the *types* section of the workflow record.

Keys must be parseable as string-encoded TypeIdentifiers.
"""

ItemsRecord = typing.List[_object.EncodedObjectDict]


class EncodedRecordDict(typing.TypedDict):
    version: VersionLiteral
    types: TypesRecord
    items: ItemsRecord
