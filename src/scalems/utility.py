"""Decorators and helper functions."""

__all__ = [
    'app',
    'command',
    'function_wrapper',
    'poll',
    'run',
    'wait',
    'ScriptEntryPoint'
]


import abc
import contextvars
import functools
import logging
import typing
import warnings
from typing import Protocol

from scalems import exceptions

from scalems.context import get_context, scope, WorkflowManager


logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


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
    raise exceptions.MissingImplementationError()


def poll():
    """Inspect the execution status of an operation.

    Inspects the execution graph state in the current context at the time of
    execution.

    Used in a work graph, this adds a non-deterministic aspect, but adds truly
    asynchronous adaptability.
    """
    raise exceptions.MissingImplementationError()


ResultType = typing.TypeVar('ResultType')


class WorkflowObject(typing.Generic[ResultType]): ...


def _unpack_work(ref: dict):
    """Temporary handler for ad hoc dict-based input.

    Unpack and serialize the nested task descriptions.

    Note: this assumes work is nested, with only one item per "layer".
    """
    assert isinstance(ref, dict)
    implementation_identifier = ref.get('implementation', None)
    message: dict = ref.get('message', None)
    if not isinstance(implementation_identifier, list) or not isinstance(message, dict):
        raise exceptions.DispatchError('Bug: bad schema checking?')

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
    raise exceptions.DispatchError('No dispatcher for this type of reference.')
    # TODO: Return an object supporting the result type interface.


@_wait.register
def _(ref: dict, *, manager):
    # First draft: monolithic implementation directs the workflow manager to add tasks and execute them.
    # TODO: Use a WorkflowManager interface from the core data model.
    if not isinstance(manager, WorkflowManager):
        raise exceptions.ProtocolError('Provided manager does not implement the required interface.')
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
        scalems.wait is primarily intended as an abstraction from https://docs.python.org/3.8/library/asyncio-eventloop.html#asyncio.loop.run_until_complete and an alternative to `await`.
    """
    context = get_context()
    if context is None:
        # Bail out.
        raise exceptions.DispatchError(str(ref))
    if not isinstance(context, WorkflowManager):
        raise exceptions.ProtocolError('Expected WorkflowManager. Got {}'.format(repr(context)))

    # Dispatch on reference type.
    return _wait(ref, manager=context)


def _run(*, work, context, **kwargs):
    """Run in current scope."""
    import asyncio
    from asyncio.coroutines import iscoroutinefunction

    # TODO: Allow custom dispatcher hook.
    if iscoroutinefunction(context.run):

        # TODO: Rearchitect the handling of *work*.
        # Don't run function until the dispatcher is active or dispatch on *work* type.
        # We can't support scalems.wait() in scalems.app as intended if dispatcher is not active.
        if callable(work):
            logger.debug('Preprocessing callable *work*.')
            # This is supposed to either get a coroutine object from *work* or allow
            # *work* the opportunity to interact with the workflow manager before dispatching begins.
            try:
                handle = work(**kwargs)
            except Exception as e:
                logger.exception('Uncaught exception in scalems.run() processing work: ' + str(e))
                raise e
        else:
            raise exceptions.DispatchError('Asynchronous workflow context expects callable work.')

        logger.debug('Creating coroutine object for workflow dispatcher.')
        # TODO: Handle in context.run() via full dispatcher implementation.
        # TODO:
        # coro = context.run(work, **kwargs)
        try:
            coro = context.run()
        except Exception as e:
            logger.exception('Uncaught exception in scalems.run() calling context.run(): ' + str(e))
            raise e

        logger.debug('Starting asyncio.run()')
        # Manage event loop directly, since asyncio.run() doesn't seem to always clean it up right.
        # TODO: Check for existing event loop.
        loop = asyncio.get_event_loop()
        try:
            task = loop.create_task(coro)
            result = loop.run_until_complete(task)
        finally:
            loop.close()
        assert loop.is_closed()

        logger.debug('Finished asyncio.run()')
    else:
        logger.debug('Starting context.run() without asyncio wrapper')
        result = context.run(work, **kwargs)
        logger.debug('Finished context.run()')
    return result


def run(work, context=None, **kwargs):
    """Execute a workflow and return the results.

    This call is not necessary if an execution manager is already running, such
    as when a workflow script is invoked with `python -m scalems.<some_executor> workflow.py`,
    when run in a Jupyter notebook (or other application with a compatible native event loop),
    or when the execution manager is launched explicitly within the script.

    `scalems.run()` may be useful if you want to embed a ScaleMS application in another
    application, or as a short-hand for execution management with the Python
    Context Manager syntax by which ScaleMS execution can be more explicitly directed.
    `scalems.run()` is analogous to (and may simply wrap a call to) `asyncio.run()`.

    As with `asyncio.run()`, `scalems.run()` is intended to be invoked (from the
    main thread) exactly once in a Python interpreter process lifetime. It is
    probably fine to call it more than once, but such a use case probably indicates
    non-standard ScaleMS software design. Nested calls to `scalems.run()` have
    unspecified behavior.

    Abstraction for :py:func:`asyncio.run()`

    Note: If we want to go this route, we should integrate with the
    asyncio event loop policy, or obtain an event loop instance and
    use it w.r.t. run_in_executor and set_task_factory.

    .. todo:: Coordinate with RP plans for event loop contexts and concurrency module executors.

    See also https://docs.python.org/3/library/asyncio-dev.html#debug-mode
    """
    # Cases, likely in appropriate order of resolution:
    # * work is a SCALEMS ItemView or Future
    # * work is a asyncio.coroutine
    # * work is a asyncio.coroutinefunction
    # * work is a regular Python callable
    # * work is None (get all work from current and/or parent workflow context)

    # TODO: Check whether coroutine is already executing and where.
    # if iscoroutine(coroutine):
    #     return asyncio.run(coroutine, **kwargs)

    # No automatic dispatching yet. Coroutine must be executable
    # in the current or provided context.
    try:
        if context is None:
            context = get_context()
        if context is get_context():
            result = _run(work=work, context=context, **kwargs)
        else:
            with scope(context):
                result = _run(work=work, context=context, **kwargs)
        return result
    except Exception as e:
        message = 'Uncaught exception in scalems.context.run(): {}'.format(str(e))
        warnings.warn(message)
        logger.warning(message)

    # TODO: Consider generalized coroutines to be dispatched through
    #     custom event loops or executors.


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