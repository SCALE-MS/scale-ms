"""SCALE-MS - Scalable Adaptive Large Ensembles of Molecular Simulations.

This package provides Python driven data flow scripting and graph execution
for molecular science computational research protocols.

Refer to https://scale-ms.readthedocs.io/ for package documentation.

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
    'app',
    # tools / commands
    'executable',
    # utilities and helpers
    'get_context',
    # 'run',
    # 'wait',
]

import logging

# Import the singleton early to avoid ambiguity under multi-threaded conditions.
from ._version import get_versions
from .context import get_context
from .subprocess import executable
from .utility import app, ScriptEntryPoint

__version__ = get_versions()['version']
del get_versions
# We do not provide a version module or API.
# Use :py:mod:`packaging.version <https://packaging.pypa.io/en/latest/version/>`
# or pkg_resources.get_distribution('gmxapi').version for richer interfaces.

logger = logging.getLogger(__name__)
logger.debug('Imported {}'.format(__name__))
