"""Test the scalems CPI helpers and metaprogramming utilities.

TODO: reconcile and merge with test_datamodel.py
"""

import json
import logging
import typing

from scalems.messages import Command, StopCommand, Control

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


def test_serialization():
    cmd = StopCommand()
    serialized = json.dumps(cmd.encode())
    obj: dict = json.loads(serialized)
    assert "control" in obj.keys()
    assert obj["control"] == "stop"


def test_deserialization():
    serialized_stop = '{"control": "stop"}'
    # Unless/until we embrace typing Literals, there is nto really a way to hint
    # the type narrowing of a string-based registration scheme in Command.decode().
    cmd = typing.cast(Control, Command.decode(json.loads(serialized_stop)))
    assert cmd.key == "control"
    assert cmd.message == "stop"
