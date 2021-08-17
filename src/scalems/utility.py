"""Decorators and helper functions."""

__all__ = [
    'app',
    'command',
    'function_wrapper',
    'make_parser',
    'parser',
    'poll',
    'wait',
    'ScriptEntryPoint'
]

import abc
import argparse
import contextvars
import functools
import logging
import typing
from typing import Protocol

from scalems import exceptions as _exceptions
from scalems.context import get_context
from ._version import get_versions
from .workflow import WorkflowManager

_scalems_version = get_versions()['version']
del get_versions

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# TODO: (Python 3.9) Use functools.cache instead of lru_cache when Py 3.9 is required.
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

    The new function returns an object with an ``output`` attribute containing the named outputs.

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


ResultType = typing.TypeVar('ResultType')


class WorkflowObject(typing.Generic[ResultType]):
    ...


def _unpack_work(ref: dict):
    """Temporary handler for ad hoc dict-based input.

    Unpack and serialize the nested task descriptions.

    Note: this assumes work is nested, with only one item per "layer".
    """
    assert isinstance(ref, dict)
    implementation_identifier = ref.get('implementation', None)
    message: dict = ref.get('message', None)
    if not isinstance(implementation_identifier, list) or not isinstance(message, dict):
        raise _exceptions.DispatchError('Bug: bad schema checking?')

    command = implementation_identifier[-1]
    logger.debug(f'Unpacking a {command}')
    # Temporary hack for ad hoc schema.
    if command == 'Executable':
        # generate Subprocess
        from scalems.subprocess import SubprocessInput, Subprocess
        input_node, task_node, output_node = message['Executable']
        kwargs = {
            'argv': input_node['data']['argv'],
            'stdin': input_node['data']['stdin'],
            'stdout': task_node['data']['stdout'],
            'stderr': task_node['data']['stderr'],
            'environment': input_node['data']['environment'],
            'resources': task_node['input']['resources']
        }
        bound_input = SubprocessInput(**kwargs)
        item = Subprocess(input=bound_input)
        yield item
        return item.uid()
    else:
        # If record bundles dependencies, identify them and yield them first.
        try:
            depends = ref['message'][command]['input']
        except AttributeError:
            depends = None
        if depends is not None:
            logger.debug(f'Recursively unpacking {depends}')
            dependency: typing.Optional[bytes] = yield from _unpack_work(depends)
        else:
            dependency = None
        if 'uid' not in ref:
            ref['uid'] = next_monotonic_integer().to_bytes(32, 'big')
        uid: bytes = ref['uid']
        if dependency is not None:
            logger.debug('Replacing explicit input in {} with reference: {}'.format(
                uid.hex(),
                dependency.hex()
            ))
            ref['message'][command]['input'] = dependency
        # Then yield the dependent item.
        yield ref
        return uid


@functools.singledispatch
def _wait(ref, *, manager):
    """Use the indicated workflow manager to resolve a reference to a workflow item."""
    raise _exceptions.DispatchError('No dispatcher for this type of reference.')
    # TODO: Return an object supporting the result type interface.


@_wait.register
def _(ref: dict, *, manager):
    # First draft: monolithic implementation directs the workflow manager to add tasks and execute them.
    # TODO: Use a WorkflowManager interface from the core data model.
    if not isinstance(manager, WorkflowManager):
        raise _exceptions.ProtocolError('Provided manager does not implement the required interface.')
    for item in _unpack_work(ref):
        view = manager.add_item(item)
        logger.debug('Added {}: {}'.format(
            view.uid().hex(),
            str(item)))
    # TODO: If dispatcher is running, wait for the results.
    # TODO: If dispatcher is not running, can we trigger it?


# def wait(ref: WorkflowObject[ResultType], **kwargs) -> ResultType:
def wait(ref):
    """Resolve a workflow reference to a local object.

    *wait* signals to the SCALE-MS framework that it is time to intervene and
    do some workflow execution management.

    ScaleMS commands return abstract references to work without waiting for the
    work to execute. Other ScaleMS commands can operate on these references,
    relying on the framework to manage data flow.

    If you need to extract a concrete result, or otherwise force data flow resolution
    (blocking the current code until execution and data transfer are complete),
    you may use scalems.wait(ref) to convert a workflow reference to a concrete
    local result.

    Note that scalems.wait() can allow the current scope to yield to other tasks.
    Developers should use scalems.wait() instead of native concurrency primitives
    when coding for dynamic data flow.
    However, the initial implementation does not inspect the context to allow
    such context-sensitive behavior.

    .. todo:: Establish stable API/CPI for tasks that create other tasks or modify the data flow graph during execution.

    scalems.wait() will produce an error if you have not configured and launched
    an execution manager in the current scope.

    .. todo:: Acquire asyncio event loop from WorkflowManager.
        scalems.wait is primarily intended as an abstraction from
        https://docs.python.org/3.8/library/asyncio-eventloop.html#asyncio.loop.run_until_complete
        and an alternative to `await`.
    """
    context = get_context()
    if context is None:
        # Bail out.
        raise _exceptions.DispatchError(str(ref))
    if not isinstance(context, WorkflowManager):
        raise _exceptions.ProtocolError('Expected WorkflowManager. Got {}'.format(repr(context)))

    # Dispatch on reference type.
    return _wait(ref, manager=context)


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
    """Utility for generating a monotonic sequence of integers across an interpreter process.

    Not thread-safe. However, threads may

    * avoid race conditions by copying the contextvars context for non-root threads
    * reproduce the sequence of the main thread by calling this function an equal
      number of times.

    Returns:
        Next integer.

    """
    value = _monotonic_integer.get()
    _monotonic_integer.set(value + 1)
    return value


_monotonic_integer = contextvars.ContextVar('_monotonic_integer', default=0)
