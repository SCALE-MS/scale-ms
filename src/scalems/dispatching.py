"""Queue management and work dispatching."""
import logging
import typing

from scalems.exceptions import APIError

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


class QueueItem(dict, typing.MutableMapping[str, typing.Union[str, bytes]]):
    """Queue items are either workflow items or control messages."""

    def _hexify(self):
        for key, value in self.items():
            if isinstance(value, bytes):
                value = value.hex()
            yield key, value

    def __str__(self) -> str:
        return str(dict(self._hexify()))


class _CommandQueueControlItem(QueueItem, typing.MutableMapping[str, str]):
    """String-encoded Command for the Executor command queue.

    Currently the only supported key is "command".

    Currently the only supported value is "stop".
    """
    _allowed: typing.ClassVar = {'command': {'stop'}}

    def __setitem__(self, k: str, v: str) -> None:
        if k in self._allowed:
            if v in self._allowed[k]:
                super().__setitem__(k, v)
            else:
                raise APIError(f'Unsupported command value: {repr(v)}')
        else:
            raise APIError(f'Unsupported command key: {repr(k)}')


class _CommandQueueAddItem(QueueItem, typing.MutableMapping[str, bytes]):
    """String-encoded add_item command for the Executor command queue.
    """
    _allowed: typing.ClassVar = {'add_item'}

    def __setitem__(self, k: str, v: str) -> None:
        if k in self._allowed:
            if isinstance(v, bytes):
                super().__setitem__(k, v)
            else:
                raise APIError(f'Unsupported add_item key: {repr(v)}')
        else:
            raise APIError(f'Unsupported command: {repr(k)}')
