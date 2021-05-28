"""Core typing information.

This module is intended to have no dependencies. It allows some core definitions to be
used in multiple modules without tightly coupling those modules.
"""
import typing

json_base_encodable_types: typing.Tuple[type, ...] = (
    dict, list, tuple, str, int, float, bool, type(None))

json_base_decoded_types: typing.Tuple[type, ...] = (
    dict, list, str, int, float, bool, type(None))
BaseEncodable = typing.Union[dict, list, tuple, str, int, float, bool, None]
BaseDecoded = typing.Union[dict, list, str, int, float, bool, None]


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
