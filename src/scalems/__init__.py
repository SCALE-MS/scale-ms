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

# Note: Even though `from scalems import *` is generally discouraged, the __all__ module attribute is useful
# to document the intended public interface *and* to indicate sort order for tools like
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#directive-automodule
__all__ = [
    # core UI
    "app",
    # tools / commands
    "executable",
    # utilities and helpers
    "get_scope",
    # 'run',
    # 'wait',
    "__version__",
    # core API
    "ScriptEntryPoint",
]

import abc
import functools
import logging
import typing

from ._version import __version__
from .subprocess import executable
from .workflow import get_scope

logger = logging.getLogger(__name__)
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
