"""SCALE-MS - Scalable Adaptive Large Ensembles of Molecular Simulations.

This package provides Python driven data flow scripting and graph execution
for molecular science computational research protocols.

Documentation is published online at https://scale-ms.readthedocs.io/.

Refer to https://github.com/SCALE-MS/scale-ms/wiki for development documentation.

Invocation:
    ScaleMS scripts describe a workflow. To run the workflow, you must tell
    ScaleMS how to dispatch the work for execution.

    For most use cases, you can run the script in the context of a particular
    execution scheme by using the ``-m`` Python command line flag to specify a
    ScaleMS execution module::

        # Execute with the default local execution manager.
        python -m scalems.local myworkflow.py
        # Execute with the RADICAL Pilot based execution manager.
        python -m scalems.radical myworkflow.py

    Execution managers can be configured and used from within the workflow script,
    but the user has extra responsibility to properly shut down the execution
    manager, and the resulting workflow may be less portable. For details, refer
    to the documentation for particular WorkflowContexts.

"""
from __future__ import annotations

# Note: Even though `from scalems import *` is generally discouraged, the __all__ module attribute is useful
# to document the intended public interface *and* to indicate sort order for tools like
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#directive-automodule
__all__ = (
    # core UI
    "app",
    # tools / commands
    "executable",
    "submit",
    # utilities and helpers
    "get_scope",
    # 'run',
    # 'wait',
    "__version__",
    # core API
    "ScriptEntryPoint",
)

import abc
import asyncio
import collections.abc
import functools
import typing
from asyncio import AbstractEventLoop
from contextvars import Context
from typing import Any
from typing import Callable

import typing_extensions

from ._version import __version__
from .subprocess import executable
from .workflow import get_scope
from .logger import logger

if typing.TYPE_CHECKING:
    import mpi4py.MPI

logger.debug("Imported {}".format(__name__))


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
    """Annotate a callable for execution by SCALEMS."""

    class App(ScriptEntryPoint):
        def __init__(self, func: typing.Callable):
            if not callable(func):
                raise ValueError("Needs a function or function object.")
            self._callable = func
            self.name = None

        def __call__(self, *args, **kwargs):
            return self._callable(*args, **kwargs)

    decorated = functools.update_wrapper(App(func), wrapped=func)

    return decorated


_P = typing_extensions.ParamSpec("_P")
_T = typing.TypeVar("_T")


class Task(typing.Protocol[_T]):
    """A Future for a submitted SCALEMS task.

    Behaves like an `asyncio.Task`, but may have additional features.

    Use with :py:mod`asyncio` utilities.
    """

    def get_loop(self) -> AbstractEventLoop:
        return super().get_loop()

    def add_done_callback(self, __fn: Callable[[asyncio.Future], object], *, context: Context | None = ...) -> None:
        super().add_done_callback(__fn, context=context)

    def cancel(self, msg: Any | None = ...) -> bool:
        return super().cancel(msg)

    def cancelled(self) -> bool:
        return super().cancelled()

    def done(self) -> bool:
        return super().done()

    def result(self) -> _T:
        return super().result()

    def exception(self) -> BaseException | None:
        return super().exception()

    def remove_done_callback(self, __fn: Callable[[asyncio.Future], object]) -> int:
        return super().remove_done_callback(__fn)


@typing.overload
def submit(
    fn: collections.abc.Callable[typing_extensions.Concatenate["mpi4py.MPI.Comm", _P], _T],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> asyncio.Task[_T]:
    ...


def submit(fn: typing.Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> Task[_T]:
    """Submit a task to be run by an executor.

    If the executor is configured for MPI tasks, a :py:class:`mpi4py.MPI.Comm`
    sub-communicator will be provided as a function argument.

    To request that the communicator be provided as a key word argument, include
    a placeholder key word argument (e.g. `comm=None`). The value will be
    replaced with a communicator instance in the executor. Otherwise, the
    executor will insert the communicator before any other positional arguments
    when calling the function (e.g. `fn(subcomm, *args, **kwargs)`).

    Not thread-safe. Call from the main thread (in which the event loop is running).

    TODO: At some point we need to be able to call this from any thread to support
     dynamic Task generation.
    """
    import scalems.execution

    current_executor = scalems.execution.current_executor.get()
    return current_executor.submit(fn, *args, **kwargs)
