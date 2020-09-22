"""Exceptions thrown by SCALE-MS are catchable as scalems.ScaleMSException.

Additional common exceptions are defined in this module.
scalems submodules may define additional exceptions, but all will be derived
from exceptions specified in scalems.exceptions.
"""

__all__ = ['ScaleMSException', 'MissingImplementationError', 'ProtocolError', 'ScopeError']

import logging

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


class ScaleMSException(Exception):
    """Base exception for scalems Python package."""


class InternalError(ScaleMSException):
    """An otherwise unclassifiable error has occurred (a bug).

    Please report the bug at https://github.com/SCALE-MS/scale-ms/issues
    """


class DispatchError(ScaleMSException):
    """SCALE-MS is unable to execute work or manage data in the requested environment."""


class DuplicateKeyError(ScaleMSException):
    """An identifier is being reused in a situation where this is not supported."""


class MissingImplementationError(ScaleMSException):
    """The expected feature is not available.

    This indicates a bug or incomplete implementation. If error message does not
    cite an existing tracked issue, please file a bug report.
    https://github.com/SCALE-MS/scale-ms/issues
    """


class ProtocolError(ScaleMSException):
    """A behavioral protocol has not been followed correctly."""


class ScopeError(ScaleMSException):
    """A command or reference is not valid in the current scope or Context."""
