"""Decorators and helper functions."""

__all__ = [
    'app',
    'command',
    'function_wrapper',
    'get_to_thread',
    'make_parser',
    'next_monotonic_integer',
    'parser',
    'poll',
    'ScriptEntryPoint'
]

import abc
import argparse
import asyncio
import contextvars
import functools
import logging
import typing
from typing import Protocol

from scalems import exceptions as _exceptions

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# TODO(Python 3.9): Use functools.cache instead of lru_cache when Py 3.9 is required.
cache = getattr(functools, 'cache', functools.lru_cache(maxsize=None))


@cache
def parser(add_help=False):
    """Get the base scalems argument parser.

    Provides a base argument parser for scripts or other module parsers.

    By default, the returned ArgumentParser is created with ``add_help=False``
    to avoid conflicts when used as a *parent* for a parser more local to the caller.
    If *add_help* is provided, it is passed along to the ArgumentParser created
    in this function.

    See Also:
         https://docs.python.org/3/library/argparse.html#parents
    """
    from . import __version__ as _scalems_version

    _parser = argparse.ArgumentParser(add_help=add_help)

    _parser.add_argument(
        '--version',
        action='version',
        version=f'scalems version {_scalems_version}'
    )

    _parser.add_argument(
        '--log-level',
        type=str.upper,
        choices=['CRITICAL',
                 'ERROR',
                 'WARNING',
                 'INFO',
                 'DEBUG'],
        help='Optionally configure console logging to the indicated level.'
    )

    _parser.add_argument(
        '--pycharm',
        action='store_true',
        default=False,
        help='Attempt to connect to PyCharm remote debugging system, where appropriate.'
    )

    _parser.add_argument(
        'script',
        metavar='script-to-run.py',
        type=str,
        help='The workflow script. Must contain a function decorated with `scalems.app`'
    )

    return _parser


def make_parser(module: str, parents: typing.Iterable[argparse.ArgumentParser] = None):
    """Make a SCALE-MS Execution Module command line argument parser.

    Args:
        module: Name of the execution module.
        parents: Optional list of parent parsers.

    If *parents* is not provided, `scalems.utility.parser` is used to generate a default.
    If *parents* _is_ provided, one of the provided parents **should** inherit from
    `scalems.utility.parser` using the *parents* parameter of
    :py:class:`argparse.ArgumentParser`.

    Notes:
        :py:data:`__package__` and :py:data:`__module__` are convenient aliases that a
        client might use to provide the *module* argument.
    """
    if parents is None:
        parents = [parser()]
    _parser = argparse.ArgumentParser(
        prog=module,
        description=f'Process {module} command line arguments.',
        usage=f'python -m {module} <{module} args> script-to-run.py.py '
              '<script args>',
        parents=parents
    )
    return _parser


class ScriptEntryPoint(abc.ABC):
    """Annotate a SCALE-MS entry point function.

    An importable Python script may decorate a callable with scalems.app to
    mark it for execution. This abstract base class provides SCALE-MS with a
    way to identify callables marked for execution and is not intended to be
    used directly.

    See :py:func:`scalems.app`
    """
    name: typing.Optional[str]

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        ...


def app(func: typing.Callable) -> typing.Callable:
    """Annotate a callable for execution by SCALEMS.


    """

    class App(ScriptEntryPoint):
        def __init__(self, func: typing.Callable):
            if not callable(func):
                raise ValueError('Needs a function or function object.')
            self._callable = func
            self.name = None

        def __call__(self, *args, **kwargs):
            return self._callable(*args, **kwargs)

    decorated = functools.update_wrapper(App(func), wrapped=func)

    return decorated


# def command(*, input_type, result_type):
#     """Get a decorator for ScaleMS Command definitions.
#
#     A ScaleMS command minimally consists of an input specification, and output
#     specification, and a callable.
#     """
#     def decorator(cls):
#         ...
#     return decorator


class Callable(Protocol):
    """This protocol describes the required function signature for a SCALE-MS command."""

    def __call__(self):
        ...


class Command(Protocol):
    """Protocol describing a SCALE-MS Command."""


def command(obj: Callable) -> Command:
    """Decorate a callable to create a SCALE-MS Command."""
    ...


def function_wrapper(output: dict = None):
    # Suppress warnings in the example code.
    # noinspection PyUnresolvedReferences
    """Generate a decorator for wrapped functions with signature manipulation.

    New function accepts the same arguments, with additional arguments required by
    the API.

    The new function returns an object with an ``output`` attribute containing the
    named outputs.

    Example:

        >>> @function_wrapper(output={'spam': str, 'foo': str})
        ... def myfunc(parameter: str = None, output=None):
        ...    output.spam = parameter
        ...    output.foo = parameter + ' ' + parameter
        ...
        >>> operation1 = myfunc(parameter='spam spam')
        >>> assert operation1.spam.result() == 'spam spam'
        >>> assert operation1.foo.result() == 'spam spam spam spam'

    Arguments:
        output (dict): output names and types

    If ``output`` is provided to the wrapper, a data structure will be passed to
    the wrapped functions with the named attributes so that the function can easily
    publish multiple named results. Otherwise, the ``output`` of the generated operation
    will just capture the return value of the wrapped function.
    """
    raise _exceptions.MissingImplementationError()


def poll():
    """Inspect the execution status of an operation.

    Inspects the execution graph state in the current context at the time of
    execution.

    Used in a work graph, this adds a non-deterministic aspect, but adds truly
    asynchronous adaptability.
    """
    raise _exceptions.MissingImplementationError()


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
        raise ValueError('`deprecated` decorator needs a *explanation*.') from e

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


def next_monotonic_integer() -> int:
    """Utility for generating a monotonic sequence of integers throughout the interpreter.

    Not thread-safe. However, threads may
    * avoid race conditions by copying the contextvars context for non-root threads, or
    * reproduce the sequence of the main thread by calling this function an equal
      number of times.

    Returns:
        Next integer.

    """
    value = _monotonic_integer.get()
    _monotonic_integer.set(value + 1)
    return value


_monotonic_integer = contextvars.ContextVar('_monotonic_integer', default=0)
_T = typing.TypeVar('_T')


class _Func_to_thread(typing.Protocol[_T]):
    """Function object type of the asyncio.to_thread callable.

    typing.Protocol supports better checking of function signatures than Callable,
    especially with respect to ``*args`` and ``**kwargs``.
    """

    def __call__(self, __func: typing.Callable[..., _T],
                 *args: typing.Any,
                 **kwargs: typing.Any) \
            -> typing.Coroutine[typing.Any, typing.Any, _T]:
        ...


@cache
def get_to_thread() -> _Func_to_thread:
    """Provide a to_thread function.

    asyncio.to_thread() appears in Python 3.9, but we only require 3.8 as of this writing.
    """
    try:
        from asyncio import to_thread as _to_thread
    except ImportError:
        async def _to_thread(__func: typing.Callable[..., _T],
                             *args: typing.Any,
                             **kwargs: typing.Any) -> _T:
            """Mock Python to_thread for Py 3.8."""
            wrapped_function: typing.Callable[[], _T] = functools.partial(__func,
                                                                          *args,
                                                                          **kwargs)
            assert callable(wrapped_function)
            loop = asyncio.get_event_loop()
            coro: typing.Awaitable[_T] = loop.run_in_executor(None, wrapped_function)
            result = await coro
            return result
    return _to_thread
