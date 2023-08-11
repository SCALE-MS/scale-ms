"""Intra-component communication support.

Message types and protocol support for control signals,
queue management, and work dispatching.

The `QueueItem` classes are simple key-value pairs for use in local message-passing
queues.

The :py:class:`~scalems.messages.Command` classes are richer structures that
ensure (de)serializability for use between the client and (remote) runtime managers.
"""
__all__ = ("QueueItem", "CommandQueueControlItem", "CommandQueueAddItem")

import logging
import typing

from scalems.exceptions import APIError

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class QueueItem(dict, typing.MutableMapping[str, typing.Any]):
    """Queue items are either workflow items or control messages.

    Control messages are indicated by the key ``'command'``, and are described
    by :py:class:`CommandQueueControlItem`.

    Workflow items are indicated by the key ``'add_item'``, and are described by
    :py:class:`CommandQueueAddItem`.
    """

    def _hexify(self):
        """Allow binary fields to be printable.

        Non-bytes values are returned without conversion.
        """
        for key, value in self.items():
            if isinstance(value, bytes):
                value = value.hex()
            yield key, value

    def __str__(self) -> str:
        return str(dict(self._hexify()))


class CommandQueueControlItem(QueueItem, typing.MutableMapping[str, str]):
    """String-encoded Command for the Executor command queue.

    Instructions for the `RuntimeManager`, intercepted and processed by
    :py:func:`scalems.execution.manage_execution()`.

    Currently, the only supported key is "command".

    Supported commands may grow to comprise a Compute Provide Interface.
    """

    _allowed: typing.ClassVar = {"command": {"hello", "stop", "version"}}

    def __setitem__(self, k: str, v: str) -> None:
        if k in self._allowed:
            if v in self._allowed[k]:
                super().__setitem__(k, v)
            else:
                raise APIError(f"Unsupported command value: {repr(v)}")
        else:
            raise APIError(f"Unsupported command key: {repr(k)}")


class CommandQueueAddItem(QueueItem, typing.MutableMapping[str, bytes]):
    """String-encoded add_item command for the Executor command queue.

    The intended payload is an item to be added to the workflow graph:
    e.g. an operation, data reference, subgraph, or something meaningful to an
    :py:class:`~scalems.execution.AbstractWorkflowUpdater`.

    In practice, the value may just be a token or identifier. In the initial
    version of the scalems protocol, the value is passed to the item editor factory
    that is obtained from :py:func:`scalems.execution.RuntimeManager.get_edit_item()`.
    """

    _allowed: typing.ClassVar = {"add_item"}

    def __setitem__(self, k: str, v: bytes) -> None:
        if k in self._allowed:
            if isinstance(v, bytes):
                super().__setitem__(k, v)
            else:
                raise APIError(f"Unsupported add_item value: {repr(v)}")
        else:
            raise APIError(f"Unsupported command: {repr(k)}")
