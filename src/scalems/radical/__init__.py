"""Workflow subpackage for ScaleMS execution dispatching to RADICAL Pilot.

Manage workflow context for RADICAL Pilot.

Command Line Invocation Example:
    ``python -m scalems.radical --resource local.localhost --venv $HOME/myvenv --access
    local myworkflow.py``

For required and optional command line arguments:
    ``python -m scalems.radical --help``

The user is largely responsible for establishing appropriate
`RADICAL Cybertools <https://radical-cybertools.github.io/>`__
(RCT) software environment at both the client side
and the execution side.

For performance and control, the canonical use case is a fully static `venv`
configuration for both the Pilot agent (and bootstrapping) interpreter, remote RCT
stack, and executed Tasks. However, the default behavior should work for most users,
in which a `venv` is created in the radical sandbox on the first connection (reused if it
already exists) and the RCT stack is updated within the Pilot sandbox for each session.

In the case of non-RCT Python dependencies,
:py:class:`~radical.pilot.Pilot` has an (evolving)
:py:func:`~radical.pilot.Pilot.prepare_env`
feature that can be used for a Task dependency
(:py:data:`~radical.pilot.TaskDescription.named_env`) to provide a dynamically
created venv with a list of requested packages.

Locally prepared package distribution archives can be used, such as by staging with the
*Pilot.stage_in* before doing *prepare_env*.

.. seealso::

    Refer to https://github.com/SCALE-MS/scale-ms/issues/141
    for the status of `scalems` support for automatic execution environment bootstrapping.

Use ``virtenv_mode=update``, ``rp_version=local`` in the RP resource definition,
and activate alternative Task venvs using ``named_env``.
Otherwise (e.g. for ``virtenv_mode=use``), the user (or client) is
responsible for maintaining venv(s) with the correct RCT stack (matching the API
used by the client-side RCT stack).

Tasks use a separately-provisioned virtual environment, if appropriate. For Python-based tasks,
the `scalems` package takes responsibility for provisioning an appropriate virtual environment
with itself and any dependencies of the workflow, unless :option:`--venv` is specified on the
command line.

See Also:
    * `scalems.dispatching`
    * `scalems.execution`
    * `scalems.radical.runtime`
    * https://github.com/SCALE-MS/scale-ms/issues/90

"""
# TODO: Consider converting to a namespace package to improve modularity of
#  implementation.

__all__ = [
    'configuration',
    'parser',
    'workflow_manager'
]

import asyncio
import functools
import logging

import scalems.execution
import scalems.subprocess
import scalems.workflow
from scalems.utility import make_parser as _make_parser
from .runtime import configuration
from .runtime import parser as _runtime_parser

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)

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
    return scalems.workflow.WorkflowManager(
        loop=loop,
        executor_factory=executor_factory,
        directory=directory
    )
