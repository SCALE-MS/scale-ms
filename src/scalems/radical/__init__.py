"""Workflow subpackage for ScaleMS execution dispatching to RADICAL Pilot.

Manage workflow context for RADICAL Pilot.

Command Line Invocation::

    python -m scalems.radical \\
        --resource local.localhost --venv $HOME/myvenv --access local myworkflow.py

For required and optional command line arguments::

    python -m scalems.radical --help

or refer to the web-based documentation for
`scalems.radical command line`.

The user is largely responsible for establishing appropriate
`RADICAL Cybertools <https://radical-cybertools.github.io/>`__
(RCT) software environment at both the client side
and the execution side. See :ref:`rp setup` in the
:doc:`install` for complete documentation.

See Also:
    Refer to https://github.com/SCALE-MS/scale-ms/issues/141
    for the status of `scalems` support for automatic execution environment bootstrapping.

See Also:
    * `scalems.messages`
    * `scalems.execution`
    * `scalems.radical.runtime`
    * https://github.com/SCALE-MS/scale-ms/issues/90

"""
# TODO: Consider converting to a namespace package to improve modularity of
#  implementation.

__all__ = ["configuration", "parser", "workflow_manager"]

import asyncio
import logging

import scalems.workflow
from ..invocation import make_parser as _make_parser
from .runtime import configuration
from .runtime import parser as _runtime_parser

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

parser = _make_parser(__package__, parents=[_runtime_parser()])


def workflow_manager(loop: asyncio.AbstractEventLoop, directory=None):
    """Manage a workflow context for RADICAL Pilot work loads.

    The rp.Session is created when the Python Context Manager is "entered",
    so the asyncio event loop must be running before then.

    To help enforce this, we use an async Context Manager, at least in the
    initial implementation. However, the implementation is not thread-safe.
    It is not reentrant, but this is not checked. We probably _do_ want to
    constrain ourselves to zero or one Sessions per environment, but we _do_
    need to support multiple Pilots and task submission scopes (_resource
    requirement groups).
    Further discussion is welcome.

    Warning:
        The importer of this module should be sure to import radical.pilot
        before importing the built-in logging module to avoid spurious warnings.
    """
    from .runtime import executor_factory

    return scalems.workflow.WorkflowManager(loop=loop, executor_factory=executor_factory, directory=directory)
