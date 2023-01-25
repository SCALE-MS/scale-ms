"""Workflow subpackage for ScaleMS execution dispatching to RADICAL Pilot.

Manage workflow context for RADICAL Pilot.

Command Line Invocation::

    python -m scalems.rp \\
        --resource local.localhost --venv $HOME/myvenv --access local myworkflow.py

For required and optional command line arguments::

    python -m scalems.rp --help

or refer to the web-based documentation for
`scalems.rp command line`.

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

"""
# TODO: Consider converting to a namespace package to improve modularity of
#  implementation.

__all__ = ["configuration", "parser", "workflow_manager"]

import asyncio
import dataclasses
import functools
import io
import logging
import os
import pathlib
import shutil
import zipfile
from collections.abc import Awaitable

import radical.pilot as rp
import radical.saga

import scalems.call
import scalems.execution
import scalems.file
import scalems.store
import scalems.subprocess
import scalems.workflow
from ..invocation import make_parser as _make_parser
from .runtime import configuration
from .runtime import parser as _runtime_parser
from .runtime import RPDispatchingExecutor as _RPDispatchingExecutor
from .runtime import get_pre_exec as _get_pre_exec

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

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

    return scalems.workflow.WorkflowManager(loop=loop, executor_factory=executor_factory, directory=directory)


@dataclasses.dataclass
class RPTaskResult:
    """A collection of data and managed objects associated with a completed RP Task.

    A rp.Task is associated with additional artifacts that are not directly tied
    to the rp.Task object, such as an arbitrary number of files that are not generally
    knowable a priori.
    """

    task_dict: dict
    """Dictionary representation from `radical.pilot.Task.as_dict()`."""

    final_state: str

    directory: radical.saga.Url
    """Resource location information for the Task working directory."""

    directory_archive: Awaitable[scalems.store.FileReference]
    """An archive of the task working directory.

    This result is separately awaitable to allow elision of unnecessary transfers.
    The exact implementation is not strongly specified; a Future usually implies
    that the result will eventually be produced, whereas a more general "awaitable"
    may not and may never be scheduled.

    A future optimization should allow individual files to be selected and remotely
    extracted, but this would require more progress on the Scale-MS structured data typing model.

    TODO: In the next iteration, use a non-local FileReference or (TBD) DirectoryReference.
    """


async def subprocess_to_rp_task(
    call_handle: scalems.call._Subprocess, dispatcher: _RPDispatchingExecutor
) -> RPTaskResult:
    """Dispatch a subprocess task through the scalems.rp execution backend.

    Get a Future for a RPTaskResult (a collection representing a completed rp.Task).

    Schedule a RP Task and wrap with asyncio. Subscribe a dependent asyncio.Task
    that can provide the intended result type and return it as the Future.
    Schedule a call-back to clean up temporary files.
    """
    subprocess_task_description = rp.TaskDescription()
    subprocess_task_description.stage_on_error = True
    subprocess_task_description.uid = call_handle.uid
    subprocess_task_description.executable = call_handle.executable
    subprocess_task_description.arguments = list(call_handle.arguments)
    subprocess_task_description.pre_exec = list(_get_pre_exec(dispatcher.configuration()))

    subprocess_task_description.input_staging = list()
    for name, ref in call_handle.input_filenames.items():
        # TODO: Localize the files to the execution site, instead, and use the
        #   (remotely valid) URI (with a COPY or LINK action).
        #   Perform transfers concurrently and await successful transfer
        #   before the submit_tasks call.
        await ref.localize()
        subprocess_task_description.input_staging.append(
            {
                "source": ref.as_uri(),
                # TODO: Find a programmatic mechanism to translate between URI and CLI arg for robustness.
                "target": "task:///" + name,
                "action": rp.TRANSFER,
            }
        )

    task_manager = dispatcher.runtime.task_manager()
    (subprocess_task,) = await asyncio.to_thread(task_manager.submit_tasks, [subprocess_task_description])
    subprocess_task_future: asyncio.Task[rp.Task] = await scalems.rp.runtime.rp_task(subprocess_task)

    # TODO: We really should consider putting timeouts on all tasks.
    subprocess_task = await subprocess_task_future
    # Note: This coroutine may never be scheduled. That's okay, for now. In the long run, though,
    # we should have a low-priority scheme for synchronizing file stores in the background.
    logger.debug(f"Task {subprocess_task.uid} sandbox: {subprocess_task.task_sandbox}.")
    archive_future = get_directory_archive(subprocess_task.task_sandbox, dispatcher=dispatcher)

    result = RPTaskResult(
        task_dict=subprocess_task.description,
        directory=subprocess_task.task_sandbox,
        final_state=subprocess_task.state,
        directory_archive=archive_future,
    )
    return result


async def subprocess_result_from_rp_task(
    subprocess: scalems.call._Subprocess, rp_task_result: RPTaskResult
) -> scalems.call.Result:
    archive_ref = await rp_task_result.directory_archive
    output_file = subprocess.output_filenames[0]

    with zipfile.ZipFile(pathlib.Path(archive_ref)) as myzip:
        with myzip.open(output_file) as fh:
            result: scalems.call.Result = scalems.call.deserialize_result(io.TextIOWrapper(fh).read())
    return result


async def get_directory_archive(
    directory: radical.saga.Url, dispatcher: _RPDispatchingExecutor
) -> scalems.store.FileReference:
    """Get a local archive of a remote directory."""
    staging_directory = dispatcher.datastore.directory.joinpath(f".scalems_output_staging_{id(directory)}")
    logger.debug(f"Preparing to stage {directory} to {staging_directory}.")

    # TODO: Don't rely on the RP Session. We should be able to do this after the
    #   Session is closed or after an interrupted Session.
    pilot = dispatcher.runtime.pilot()
    # TODO: Can we avoid serializing more than once? Such as with `rp.TARBALL`?
    await asyncio.to_thread(
        pilot.stage_out,
        sds=[
            {
                "source": directory,
                "target": staging_directory.as_uri(),
                "action": rp.TRANSFER,
                "flags": rp.CREATE_PARENTS | rp.RECURSIVE,
            }
        ],
    )
    archive_name = dispatcher.datastore.directory.joinpath(f".scalems_output_staging_{id(directory)}")
    # WARNING: shutil.make_archive is not threadsafe until Python 3.10.6
    # TODO: Improve locking scheme or thread safety.
    with dispatcher.datastore._update_lock:
        archive_path = await asyncio.to_thread(
            shutil.make_archive,
            archive_name,
            "zip",
            root_dir=staging_directory,
            base_dir=".",
        )
    try:
        file_ref = await dispatcher.datastore.add_file(scalems.file.describe_file(archive_path))
    finally:
        await asyncio.to_thread(shutil.rmtree, staging_directory)
        await asyncio.to_thread(os.unlink, archive_path)
    return file_ref
