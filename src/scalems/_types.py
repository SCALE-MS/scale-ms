"""Core typing information.

This module is intended to have no dependencies. It allows some core definitions to be
used in multiple modules without tightly coupling those modules.
"""
import typing

json_base_encodable_types: typing.Tuple[type, ...] = (dict, list, tuple, str, int, float, bool, type(None))

json_base_decoded_types: typing.Tuple[type, ...] = (dict, list, str, int, float, bool, type(None))
BaseEncodable = typing.Union[dict, list, tuple, str, int, float, bool, None]
BaseDecoded = typing.Union[dict, list, str, int, float, bool, None]
