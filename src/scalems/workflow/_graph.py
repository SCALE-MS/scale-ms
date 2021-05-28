__all__ = ['DAGState']

import functools
import logging
import typing
import weakref

from scalems.exceptions import APIError
from scalems.exceptions import MissingImplementationError
from scalems.identifiers import Identifier
from ._object import Data
from ._object import Object

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


class DAGState:
    """Maintain the workflow directed acyclic graph structure and state.

    The DAG state remains topologically valid when modified exclusively through the
    member functions.
    """
    _nodes: typing.MutableMapping[Identifier, Object]
    """Managed workflow objects."""
    _labels: typing.MutableMapping[str, Identifier]
    """Map user-provided strings to SCALE-MS workflow objects."""

    def __init__(self):
        self._labels = weakref.WeakValueDictionary()
        self._nodes = dict()

    @functools.singledispatchmethod
    def add_data(self, obj):
        raise APIError(f'No handlers available for {repr(obj)}.')

    # Note: We need a real class or abc.ABC to register with functools dispatching.
    @add_data.register  # type: ignore[no-redef]
    def _(self, obj: Data):
        raise MissingImplementationError

    def data(self) -> typing.Iterable[Data]:
        for node in self._nodes:
            if isinstance(node, Data):
                # TODO: Be careful about what aspects of nodes are mutable.
                yield node
