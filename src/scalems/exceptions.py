"""Exceptions thrown by SCALE-MS are catchable as scalems.ScaleMSException.

Additional common exceptions are defined in this module.
scalems submodules may define additional exceptions, but all will be derived
from exceptions specified in scalems.exceptions.
"""

import logging as _logging
import typing

logger = _logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class ScaleMSError(Exception):
    """Base exception for scalems package errors.

    Users should be able to use this base class to catch errors
    emitted by SCALE-MS.
    """


class ScaleMSWarning(Warning):
    """Base Warning for scalems package warnings.

    Users and testers should be able to use this base class to filter
    warnings emitted by SCALE-MS.
    """


class InternalError(ScaleMSError):
    """An otherwise unclassifiable error has occurred (a bug).

    Please report the bug at https://github.com/SCALE-MS/scale-ms/issues
    """


class APIError(ScaleMSError):
    """Specified interfaces are being violated."""


class ContextError(ScaleMSError):
    """A Context operation could not be performed."""


class DispatchError(ScaleMSError):
    """SCALE-MS is unable to execute work or manage data in the requested environment."""


class DuplicateKeyError(ScaleMSError):
    """An identifier is being reused in a situation where this is not supported."""


class MissingImplementationError(ScaleMSError):
    """The expected feature is not available.

    This indicates a bug or incomplete implementation. If error message does not
    cite an existing tracked issue, please file a bug report.
    https://github.com/SCALE-MS/scale-ms/issues
    """


class ProtocolError(ScaleMSError):
    """A behavioral protocol has not been followed correctly."""


class ScopeError(ScaleMSError):
    """A command or reference is not valid in the current scope or Context."""


class ProtocolWarning(ScaleMSWarning):
    """Unexpected behavior is detected that is not fatal, but which may indicate a bug."""


def deprecated(explanation: str):
    """Mark a deprecated definition.

    Wraps a callable to issue a DeprecationWarning when called.

    Use as a parameterized decorator::

        @deprecated("func is deprecated because...")
        def func():
            ...

    """
    try:
        _message = str(explanation)
        assert len(_message) > 0
    except Exception as e:
        raise ValueError("`deprecated` decorator needs a *explanation*.") from e

    def decorator(func: typing.Callable):
        import functools

        def deprecation(message):
            import warnings

            warnings.warn(message, DeprecationWarning, stacklevel=2)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            deprecation(_message)
            return func(*args, **kwargs)

        return wrapper

    return decorator
