"""Test the scalems CPI helpers and metaprogramming utilities.

May cover both the `scalems.messages` and `scalems.cpi` modules.

TODO: reconcile and merge with test_datamodel.py
"""

import logging

from scalems.cpi import stop, to_raptor_task_metadata, from_raptor_task_metadata

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


def test_encoding():
    cmd = stop()
    encoded = to_raptor_task_metadata(cmd)
    obj: dict = from_raptor_task_metadata(encoded)
    assert obj["operation"] == "stop"
    assert obj["operand"] is None
    # TODO: Test more elaborate message structures.
