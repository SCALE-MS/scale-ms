"""Specify the schema for encoded objects in the workflow record.

Note that object representations are not required to be completely self-contained.
References to other workflow items may be used in place of data values.
"""
__all__ = [
    'EncodedObjectDict',
    'EncodedValue'
]

import typing

from scalems.encoding._type import EncodedShape
from scalems.encoding._type import EncodedTypeIdentifier

Identity = typing.NewType('Identity', str)
"""Identity is either a UUID or SHA-256 hash.

A UUID is encoded as ``uid.UUID.__str__()``.

A SHA256 hash is encoded as ``hashlib.sha256().digest.hex()``.
"""

EncodedValue = typing.Union[
    None,
    list,
    str
]
"""Allowed variants for recorded data values.

Encoded data is held in a ``list``. Dimensions are commensurate with the value
of *shape*.

Field values may simply refer to other objects managed with the workflow. References
are encoded as strings.

Schema for specific object types may allow fields to be optional, in which case
``None`` is a valid value.
"""


class EncodedObjectDict(typing.TypedDict):
    """Dictionary schema for encoded workflow objects.

    The Object Type implementation (according to *type*) determines how *identity* is
    calculated and what *data* may contain.

    This class definition is only for illustration and static type checking. It is a
    plain ``dict`` at run time.
    """
    label: typing.Optional[str]
    identity: Identity
    type: EncodedTypeIdentifier
    shape: EncodedShape
    data: typing.Mapping[str, EncodedValue]
