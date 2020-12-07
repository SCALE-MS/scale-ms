"""Decorators and helper functions."""

import abc
import functools
import typing
from typing import Protocol


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

def command(obj: Callable) -> Command:
    """Decorate a callable to create a SCALE-MS Command."""
    ...
