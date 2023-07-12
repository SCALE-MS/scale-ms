from __future__ import annotations

import asyncio
import copy
import dataclasses
import functools
import io
import json
import logging
import os
import pathlib
import shutil
import tempfile

import threading
import typing
import weakref
import zipfile
from typing import Awaitable

import packaging.version
import radical.saga
import radical.utils
from radical import pilot as rp

import scalems.call
import scalems.exceptions
import scalems.file
import scalems.radical
import scalems.store
import scalems.subprocess
import scalems.workflow
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import ScaleMSError
from scalems.identifiers import TypeIdentifier
from scalems.radical.runtime_configuration import get_pre_exec
from scalems.radical.exceptions import RPInternalError
from scalems.radical.session import RmInfo

if typing.TYPE_CHECKING:
    from scalems.radical.runtime import RPDispatchingExecutor

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))


class RPTaskFailure(ScaleMSError):
    """Error in radical.pilot.Task execution.

    Attributes:
        failed_task: A dictionary representation of the failed task.

    TODO: What can/should we capture and report from the failed task?
    """

    failed_task: dict

    def __init__(self, *args, task: rp.Task):
        super().__init__(*args)
        self.failed_task = task.as_dict()


class RPFinalTaskState:
    # TODO: Provide a bridge between the threading and asyncio Event primitives so that
    #  we can effectively `await` for the events. Presumably this would look like an
    #  asyncio.Future created from a threading.Future of a watcher of the three Events.
    #  Use asyncio.wrap_future() to wrap a threading.Future to an asyncio.Future.
    #  If there is not an adapter, we can loop.run_in_executor() to run the
    #  threading.Event watcher in a separate thread.
    # For future reference, check out the `sched` module.
    def __init__(self):
        self.canceled = threading.Event()
        self.done = threading.Event()
        self.failed = threading.Event()

    def __bool__(self):
        return self.canceled.is_set() or self.done.is_set() or self.failed.is_set()

    def __repr__(self):
        return (
            f"<{self.__class__.__qualname__} "
            f"canceled={self.canceled.is_set()} "
            f"done={self.done.is_set()} "
            f"failed={self.failed.is_set()}>"
        )


def _rp_callback(
    obj: rp.Task,
    state,
    cb_data: weakref.ReferenceType = None,
    final: RPFinalTaskState = None,
):
    """Prototype for RP.Task callback.

    To use, partially bind the *final* parameter (with `functools.partial`) to get a
    callable with the RP.Task callback signature.

    Register with *task* to be called when the rp.Task state changes.
    """
    if final is None:
        raise APIError("This function is strictly for dynamically prepared RP callbacks through " "functools.partial.")
    logger.debug(f"Callback triggered by {repr(obj)} state change to {repr(state)}.")
    try:
        # Note: assertions and exceptions are not useful in RP callbacks.
        if state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED):
            # TODO: Pending https://github.com/radical-cybertools/radical.pilot/issues/2444
            # tmgr: rp.TaskManager = obj.tmgr
            # ref = cb_data()
            # tmgr.unregister_callback(cb=ref, metrics='TASK_STATE',
            #                          uid=obj.uid)
            logger.debug(f"Recording final state {state} for {repr(obj)}")
            # Consider alternatives that are easier to await for:
            #   * use an asyncio.Event instead, and use loop.call_soon_threadsafe(<event>.set)
            #   * insert a layer so that the RPFinalTaskState object can easily
            #     set its own awaitable "done" status when updated
            if state == rp.states.DONE:
                final.done.set()
            elif state == rp.states.CANCELED:
                final.canceled.set()
            elif state == rp.states.FAILED:
                final.failed.set()
            else:
                logger.error("Bug: logic error in state cases.")
    except Exception as e:
        logger.error(f"Exception encountered during rp.Task callback: {repr(e)}")


async def _rp_task_watcher(task: rp.Task, final: RPFinalTaskState, ready: asyncio.Event) -> rp.Task:  # noqa: C901
    """Manage the relationship between an RP.Task and a scalems Future.

    Cancel the RP.Task if this task or the scalems.Future is canceled.

    Publish the RP.Task result or cancel the scalems.Future if the RP.Task is
    done or canceled.

    Arguments:
        task: RADICAL Pilot Task, submitted by caller.
        final: thread-safe event handler for the RP task call-back to announce it has run.
        ready: output parameter, set when coroutine has run enough to perform its
            responsibilities.

    Returns:
        *task* in its final state.

    An asyncio.Future based on this coroutine has very similar semantics to the
    required *future* argument, but this is subject to change.
    The coroutine is intended to facilitate progress of the task,
    regardless of the rp.Task results. The provided *future* allows the rp.Task
    results to be interpreted and semantically translated. rp.Task failure is
    translated into an exception on *future*. The *future* has a different
    exposure than the coroutine return value, as well: again, the *future* is
    connected to the workflow item and user-facing interface, whereas this
    coroutine is a detail of the task management. Still, these two modes of output
    are subject to revision without notice.

    Caller should await the *ready* event before assuming the watcher task is doing its
    job.
    """

    async def wait_for_final(state: RPFinalTaskState) -> RPFinalTaskState:
        """Function to watch for final event.

        This is a poor design. We should replace with a queuing system for state
        transitions as described in scalems.workflow.
        """
        while not state:
            # TODO: What is the right adapter between asyncio and threading event waits?
            await asyncio.sleep(0.05)
        return state

    event_watcher = asyncio.create_task(wait_for_final(final))

    try:
        ready.set()

        while not event_watcher.done():
            # TODO(#96): Use a control thread to manage *threading* primitives and
            #  translate to asyncio primitives.
            # Let the watcher wake up periodically to check for suspicious state.

            _rp_task_was_complete = task.state in rp.FINAL

            done, pending = await asyncio.wait([event_watcher], timeout=60.0, return_when=asyncio.FIRST_COMPLETED)

            if _rp_task_was_complete and not event_watcher.done():
                event_watcher.cancel()
                raise RPInternalError(
                    f"RP Callbacks are taking too long to complete. Abandoning {repr(task)}. Please report bug."
                )

            if event_watcher in done:
                assert final
                logger.debug(f"Handling finalization for RP task {task.uid}.")
                if final.failed.is_set():
                    logger.error(f"{task.uid} stderr: {task.stderr}")
                    logger.info(f"Failed {task.uid} working directory: {task.task_sandbox}")
                    if logger.getEffectiveLevel() <= logging.DEBUG:
                        for key, value in task.as_dict().items():
                            logger.debug(f"    {key}: {str(value)}")
                    message = f"{task.uid} failed."
                    if task.exit_code is not None:
                        message += f" Exit code {task.exit_code}."
                    if task.stderr is not None:
                        message += f" stderr: {task.stderr}"
                    if task.exception is not None:
                        message += f" Exception: {task.exception}"
                    raise RPTaskFailure(message, task=task)
                elif final.canceled.is_set():
                    # Act as if RP called Task.cancel() on us.
                    raise asyncio.CancelledError()
                assert final.done.is_set()

                logger.debug(f"Publishing results from RP Task {task.uid}.")
                # TODO: Manage result type.
                return task

        raise scalems.exceptions.InternalError("Logic error. This line should not have been reached.")

    except asyncio.CancelledError as e:
        logger.debug(f"Received cancellation in watcher task for {repr(task)}")
        if task.state not in rp.CANCELED:
            logger.debug(f"Propagating cancellation to {repr(task)}.")
            task.cancel()
        raise e


async def rp_task(rptask: rp.Task) -> asyncio.Task[rp.Task]:
    """Mediate between a radical.pilot.Task and an asyncio.Future.

    Schedule an asyncio Task to receive the result of the RP Task. The asyncio
    Task must also make sure that asyncio cancellation propagates to the rp.Task.cancel,
    and vice versa.

    This function should be awaited immediately to make sure the necessary call-backs
    get registered. The result will be an asyncio.Task, which should be awaited
    separately.

    Internally, this function provides a call-back to the rp.Task. The call-back
    provided to RP cannot directly call asyncio.Future methods (such as set_result() or
    set_exception()) because RP will be making the call from another thread without
    mediation by the asyncio event loop.

    As such, we provide a thread-safe event handler to propagate the
    RP Task call-back to to this asyncio.Task result.
    (See :py:func:`_rp_callback()` and :py:class:`RPFinalTaskState`)

    Canceling the returned task will cause *rptask* to be canceled.
    Canceling *rptask* will cause this task to be canceled.

    Arguments:
        rptask: RADICAL Pilot Task that has already been submitted.

    Returns:
        A Task that, when awaited, returns the rp.Task instance in its final state.
    """
    if not isinstance(rptask, rp.Task):
        raise TypeError("Function requires a RADICAL Pilot Task object.")

    final = RPFinalTaskState()
    callback = functools.partial(_rp_callback, final=final)
    functools.update_wrapper(callback, _rp_callback)

    # Note: register_callback() does not provide a return value to use for
    # TaskManager.unregister_callback and we cannot provide *callback* with a reference
    # to itself until after it is created, so we will get a reference here that we can
    # provide through the *cb_data* argument of rp.Task callbacks.
    cb_data: weakref.ReferenceType = weakref.ref(callback)

    rptask.register_callback(callback, cb_data=cb_data, metric=rp.constants.TASK_STATE)

    if rptask.state in rp.FINAL:
        # rptask may have reached FINAL state before callback was registered.
        # radical.pilot.Task.register_callback does not guarantee that callbacks
        # will be called if registered after task completion.
        # Call it once. For simplicity, let the task_watcher logic proceed normally.
        logger.warning(f"RP Task {repr(rptask)} finished suspiciously fast.")
        callback(rptask, rptask.state, cb_data)

    watcher_started = asyncio.Event()
    waiter = asyncio.create_task(watcher_started.wait())
    wrapped_task = asyncio.create_task(_rp_task_watcher(task=rptask, final=final, ready=watcher_started))

    # Make sure that the task is cancellable before returning it to the caller.
    await asyncio.wait((waiter, wrapped_task), return_when=asyncio.FIRST_COMPLETED)
    if wrapped_task.done():
        # Let CancelledError propagate.
        e = wrapped_task.exception()
        if e is not None:
            raise e
    # watcher_task.
    return wrapped_task


@dataclasses.dataclass
class RPTaskResult:
    """A collection of data and managed objects associated with a completed RP Task.

    A rp.Task is associated with additional artifacts that are not directly tied
    to the rp.Task object, such as an arbitrary number of files that are not generally
    knowable a priori.

    Design note:
        This data structure is intentionally ignorant of wrapped Python functions.
        We should try to reconcile our multiple notions of RP task management as soon
        as possible. We should be careful to distinguish classes for managing traditional
        *executable* RP tasks from Python tasks managed with raptor functionality.
    """

    uid: str
    """:py:attr:`radical.pilot.Task.uid` identifier."""

    task_dict: dict
    """Dictionary representation from :py:func:`radical.pilot.Task.as_dict()`."""

    exit_code: int
    """Exit code of the executable task."""

    final_state: str
    """Final state of the `radical.pilot.Task`."""

    directory: radical.utils.Url
    """Resource location information for the Task working directory.

    Constructed from the filesystem_endpoint. See :doc:`design/localization`.
    """

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
    call_handle: scalems.call._Subprocess, dispatcher: RPDispatchingExecutor
) -> RPTaskResult:
    """Dispatch a subprocess task through the `scalems.radical` execution backend.

    Get a Future for a RPTaskResult (a collection representing a completed rp.Task).

    Schedule a RP Task and wrap with asyncio. Subscribe a dependent asyncio.Task
    that can provide the intended result type and return it as the Future.
    Schedule a call-back to clean up temporary files.

    Args:
        call_handle (scalems.call._Subprocess): result of a ``await`` on a
            :py:func:`scalems.call.function_call_to_subprocess()`
        dispatcher (RPDispatchingExecutor): An active execution manager.

    TODO:
        This logic should be integrated into a :py:func:`WorkflowManager.submit()`
        stack (currently :py:func:`WorkflowManager.add_item()`)
        and `manage_execution` loop. We really should have special handling for
        wrapped function calls as distinguished from command line executables
        through "overloads" (dispatching) of the task submission.
        Also, we should not be exposing the *dispatcher* to the user.
    """
    config = dispatcher.configuration()
    if config is None:
        raise APIError("subprocess_to_rp_task() needs an active run time.")

    subprocess_dict = dict(
        stage_on_error=True,
        uid=call_handle.uid,
        executable=call_handle.executable,
        arguments=list(call_handle.arguments),
        pre_exec=list(get_pre_exec(dispatcher.configuration())),
        mode=rp.TASK_EXECUTABLE,
    )
    if config.enable_raptor:
        subprocess_dict["raptor_id"] = dispatcher.runtime.raptor.uid
    # Capturing stdout/stderr is a potentially unusual or unexpected behavior for
    # a Python function runner, and may collide with user assumptions or native
    # behaviors of third party tools. We will specify distinctive names for the RP
    # output capture files in the hope of clarifying the component responsible for
    # these files.
    subprocess_dict["stdout"] = "_scalems_stdout.txt"
    subprocess_dict["stderr"] = "_scalems_stderr.txt"

    # Note: We could save wall time in the job by pre-staging files, but we are
    # deferring that for future optimization.
    # Ref: https://github.com/SCALE-MS/scale-ms/issues/316
    await asyncio.gather(*tuple(ref.localize() for ref in call_handle.input_filenames.values()))
    subprocess_dict["input_staging"] = [
        {
            "source": ref.as_uri(),
            # TODO: Find a programmatic mechanism to translate between URI and CLI arg for robustness.
            "target": f"task:///{name}",
            "action": rp.TRANSFER,
        }
        for name, ref in call_handle.input_filenames.items()
    ]

    # We just pass the user-provided requirements through, but we have to reject
    # any that collide with parameters we expect to generate.
    for param, value in call_handle.requirements.items():
        if param in subprocess_dict:
            raise ValueError(f"Cannot overwrite {param}. Task['{param}'] is {subprocess_dict[param]}")
        else:
            subprocess_dict[param] = value

    try:
        subprocess_task_description = rp.TaskDescription(from_dict=subprocess_dict).verify()
    except radical.utils.typeddict.TDKeyError as e:
        raise ValueError("Invalid attribute for RP TaskDescription.") from e

    task_cores = subprocess_task_description.cores_per_rank * subprocess_task_description.cpu_processes
    rm_info: RmInfo = await dispatcher.runtime.resources
    pilot_cores = rm_info["requested_cores"]
    # TODO: Account for Worker cores.
    if config.enable_raptor:
        raptor_task: rp.Task = dispatcher.runtime.raptor
        raptor_cores = raptor_task.description["ranks"] * raptor_task.description["cores_per_rank"]
    else:
        raptor_cores = 0
    if task_cores > (available_cores := pilot_cores - raptor_cores):
        raise ValueError(f"Requested {task_cores} for {call_handle.uid}, but at most {available_cores} are available.")

    # TODO: Find a better way to express these three lines.
    # This seems like it should be a subscription by the local workflow context to the
    # RP dispatching workflow context. Something like
    #     supporting_task = await rp_manager.add_task(task_description)
    #     supporting_task.add_done_callback(...)
    task_manager = dispatcher.runtime.task_manager()
    (submitted_task,) = await asyncio.to_thread(task_manager.submit_tasks, [subprocess_task_description])
    subprocess_task_future: asyncio.Task[rp.Task] = await rp_task(submitted_task)

    # TODO: We really should consider putting timeouts on all tasks.
    subprocess_task: rp.Task = await subprocess_task_future

    logger.debug(f"Task {subprocess_task.uid} sandbox: {subprocess_task.task_sandbox}.")

    sandbox: radical.saga.Url = copy.deepcopy(subprocess_task.task_sandbox)
    if not isinstance(sandbox, radical.saga.Url):
        logger.debug(f"Converting {repr(sandbox)} to expected Url type.")
        sandbox = radical.saga.Url(sandbox)

    # TODO: Schedule the task to create the remote archive, separating the archive retrieval.
    #   Get a Future for the saga URL (`done` when the archive exists remotely).
    #   In a later implementation, get a Future[FileReference] that is non-local.
    #   A refactoring of this wrapper can immediately schedule retrieval to support
    #   the packaged result.
    # Note: We need to find language to distinguish a Future that is scheduled from
    #   an awaitable that isn't. The native Coroutine is not sufficient because Coroutines
    #   are expected to be scheduled eventually and to be awaited at some point.
    #   Can we allow all potential outputs to be scheduled, but just at low priority somehow?
    #   Or can this be a formalization of our notion of "localize" as an awaitable that
    #   changes the state of the parent object?
    archive_future = asyncio.create_task(
        get_directory_archive(sandbox, dispatcher=dispatcher),
        name=f"archive-{subprocess_task.uid}",
    )
    # In the future, we can reduce latency by separating the transfers for requested results
    # from transfers for the full archive. (Use scalems.call._Subprocess.output_filenames.)
    result = RPTaskResult(
        uid=subprocess_task.uid,
        exit_code=subprocess_task.exit_code,
        task_dict=subprocess_task.description,
        directory=sandbox,
        final_state=subprocess_task.state,
        directory_archive=archive_future,
    )
    return result


async def wrapped_function_result_from_rp_task(
    subprocess: scalems.call._Subprocess, rp_task_result: RPTaskResult
) -> scalems.call.CallResult:
    """

    Once `subprocess_to_rp_task` has produced a `RPTaskResult`
    for a `scalems.call._Subprocess`,
    we produce a `scalems.call.CallResult` with the help of some file transfers.

    Args:
        subprocess: the result of a `scalems.call.function_call_to_subprocess`
        rp_task_result: the result of a `subprocess_to_rp_task` for the *subprocess*

    Returns:
        localized result from :py:func:`scalems.call.function_call_to_subprocess()`

    """
    if rp_task_result.final_state != rp.DONE:
        # Note: it is possible that the executable task could fail for reasons other than a Python Exception.
        logger.info(f"Task for {subprocess.uid} in final state {rp_task_result.final_state}.")
        logger.debug(repr(rp_task_result))
    # It looks like this should be encapsulated with the workflow management details
    # of the dispatched work item or further specialized for this specific task type.
    # It doesn't make sense that we should need both the _Subprocess and
    # RPTaskResult objects to be passed by the caller.
    # Also, we don't have access to the requested *outputs* in order to optimize out transfers
    # without adding yet another argument.
    archive_ref = await rp_task_result.directory_archive
    # TODO: Collaborate with scalems.call to agree on output filename.
    output_file = subprocess.output_filenames[0]

    with zipfile.ZipFile(pathlib.Path(archive_ref)) as myzip:
        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(f"Opening ZipFile {myzip.filename}: {', '.join(myzip.namelist())}")
        with myzip.open(output_file) as fh:
            partial_result: scalems.call.CallResult = scalems.call.deserialize_result(io.TextIOWrapper(fh).read())
    if partial_result.exception is not None:
        assert rp_task_result.final_state != rp.DONE
        logger.error(f"Subprocess {subprocess.uid} encountered Exception: {partial_result.exception}")
    # The remotely packaged result was not aware of the task environment managed by RP.
    # Update and augment the managed file details.
    stdout = copy.deepcopy(rp_task_result.directory).set_path(
        os.path.join(rp_task_result.directory.path, rp_task_result.task_dict["stdout"])
    )
    stderr = copy.deepcopy(rp_task_result.directory).set_path(
        os.path.join(rp_task_result.directory.path, rp_task_result.task_dict["stderr"])
    )

    result = dataclasses.replace(partial_result, stdout=str(stdout), stderr=str(stderr), directory=archive_ref.as_uri())
    return result


async def get_directory_archive(
    directory: radical.saga.Url, dispatcher: RPDispatchingExecutor
) -> scalems.store.FileReference:
    """Get a local archive of a remote directory.

    TODO:
        Let this be absorbed into a collaboration between WorkflowManagement
        contexts and their local and remote data stores.
    """
    # TODO: Let FileStore tell us what directory to use.
    staging_directory = dispatcher.datastore.directory.joinpath(f"_scalems_output_staging_{id(directory)}")
    logger.debug(f"Preparing to stage {directory} to {staging_directory}.")
    assert not staging_directory.exists()

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
    # Work around https://github.com/radical-cybertools/radical.pilot/issues/2823
    if packaging.version.parse(rp.version) < packaging.version.parse("1.21"):
        while True:
            try:
                staging_directory.stat()
            except FileNotFoundError:
                logger.error(f"Waiting for {staging_directory} to appear...")
                await asyncio.sleep(1.0)
            else:
                break
    try:
        with tempfile.NamedTemporaryFile(mode="wb") as tmp:
            await asyncio.to_thread(
                write_archive, filehandle=tmp, root_dir=staging_directory, relative_to=staging_directory
            )
            tmp.flush()
            archive_path = pathlib.Path(tmp.name).resolve()
            file_ref = await dispatcher.datastore.add_file(scalems.file.describe_file(archive_path))
    finally:
        await asyncio.to_thread(shutil.rmtree, staging_directory)
    return file_ref


def _add_to_archive(archive: zipfile.ZipFile, source: pathlib.Path, relative_to: pathlib.Path):
    destination = source.relative_to(relative_to)
    if source.is_dir():
        for path in source.iterdir():
            _add_to_archive(archive, path, relative_to)
    else:
        if not source.is_file():
            logger.warning(
                "Directory contains unusual filesystem object. "
                f"Attempting to write {source} to {destination} in {archive.filename}"
            )
        archive.write(filename=source, arcname=destination)


def write_archive(*, filehandle, root_dir: pathlib.Path, relative_to: pathlib.Path):
    """Write a ZipFile archive.

    Args:
        filehandle: file-like object to write the archive to
        root_dir: Base of the directory tree to archive
        relative_to: Path prefix to strip from *root_dir* when constructing the paths in the archive.
    """
    assert root_dir.is_dir()
    with zipfile.ZipFile(filehandle, mode="w") as archive:
        _add_to_archive(archive, source=root_dir, relative_to=relative_to)
        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(f"Wrote to {archive.filename}: {', '.join(archive.namelist())}")


def _describe_legacy_task(item: scalems.workflow.Task, pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    For a "raptor" style task, see _describe_raptor_task()
    """
    subprocess_type = TypeIdentifier(("scalems", "subprocess", "SubprocessTask"))
    assert item.description().type() == subprocess_type
    input_data = item.input
    task_input = scalems.subprocess.SubprocessInput(**input_data)
    args = list([arg for arg in task_input.argv])
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    task_description = rp.TaskDescription(
        from_dict=dict(
            executable=args[0],
            arguments=args[1:],
            stdout=str(task_input.stdout),
            stderr=str(task_input.stderr),
            pre_exec=pre_exec,
        )
    )
    uid: str = item.uid().hex()
    task_description.uid = uid

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = list(task_input.inputs.values())
    task_description.output_staging = [
        {
            "source": str(task_input.stdout),
            "target": os.path.join(uid, pathlib.Path(task_input.stdout).name),
            "action": rp.TRANSFER,
        },
        {
            "source": str(task_input.stderr),
            "target": os.path.join(uid, pathlib.Path(task_input.stderr).name),
            "action": rp.TRANSFER,
        },
    ]
    task_description.output_staging += task_input.outputs.values()

    return task_description


def _describe_raptor_task(item: scalems.workflow.Task, raptor_id: str, pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    The TaskDescription will be submitted to the named *scheduler*,
    where *scheduler* is the UID of a task managing the life of a rp.raptor.Master
    instance.

    Caller is responsible for ensuring that *scheduler* is valid.
    """
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    # Ref: scalems_rp_master._RaptorTaskDescription
    task_description = rp.TaskDescription(
        from_dict=dict(executable="scalems", pre_exec=pre_exec)
        # This value is currently ignored, but must be set.
    )
    task_description.uid = item.uid()
    task_description.raptor_id = str(raptor_id)
    # Example work would be the JSON serialized form of the following dictionary.
    # {'mode': 'call',
    #  'cores': 1,
    #  'timeout': 10,
    #  'data': {'method': 'hello',
    #           'kwargs': {'world': uid}}}
    #
    # Maybe something like this:
    # work_dict = {
    #     'mode': 'scalems',
    #     'cores': 1,
    #     'timeout': 10,
    #     'data': item.serialize()
    # }
    work_dict = {
        "mode": "exec",
        "cores": 1,
        "timeout": None,
        "data": {"exe": item.input["argv"][0], "args": item.input["argv"][1:]},
    }
    task_description.arguments = [json.dumps(work_dict)]

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = []
    task_description.output_staging = []

    return task_description


async def submit(
    *,
    item: scalems.workflow.Task,
    task_manager: rp.TaskManager,
    pre_exec: list,
    raptor_id: typing.Optional[str] = None,
) -> asyncio.Task:
    """Dispatch a WorkflowItem to be handled by RADICAL Pilot.

    Submits an rp.Task and returns an asyncio.Task watcher for the submitted task.

    Creates a Future, registering a done_callback to publish the task result with
    *item.set_result()*.

    A callback is registered with the rp.Task to set an Event on completion. An
    asyncio.Task watcher task monitors the Event(s) and gets a reference to an
    asyncio.Future through which results can be published to the scalems workflow item.
    (Currently the Future is created in this function, but should probably be acquired
    directly from the *item* itself.) The watcher task waits for the rp.Task
    finalization event or for the Future to be cancelled. Periodically, the watcher
    task "wakes up" to check if something has gone wrong, such as the rp.Task
    completing without setting the finalization event.

    Caveats:
        There is an unavoidable race condition in the check performed by the watcher
        task. We don't know how long after an rp.Task completes before its callbacks
        will run and we can't check whether the callback has been scheduled. The
        watcher task cannot be allowed to complete successfully until we know that the
        callback has either run or will never run.

        The delay between the rp.Task state change and the callback execution should be
        less than a second. We can allow the watcher to wake up occasionally (on the
        order of minutes), so we can assume that it will never take more than a full
        iteration of the waiting loop for the callback to propagate, unless there is a
        bug in RP. For simplicity, we can just note whether ``rptask.state in rp.FINAL``
        before the watcher goes to sleep and raise an error if the callback is not
        triggered in an iteration where we have set such a flag.

        If our watcher sends a cancellation to the rp.Task, there is no need to
        continue to monitor the rp.Task state and the watcher may exit.

    Args:
        item: The workflow item to be submitted
        task_manager: A `radical.pilot.TaskManager` instance
            through which the task should be submitted.
        pre_exec: :py:data:`radical.pilot.Task.pre_exec` prototype.
        raptor_id (str): The string name of the "scheduler," corresponding to
            the UID of a Task running a rp.raptor.Master (if Raptor is enabled).

    Returns:
        asyncio.Task: a "Future[rp.Task]" for a rp.Task in its final state.

    The caller *must* await the result of the coroutine to obtain an asyncio.Task that
    can be cancelled or awaited as a proxy to direct RP task management. The Task will
    hold a coroutine that is guaranteed to already be running, failed, or canceled. The
    caller should check the status of the task immediately before making assumptions
    about whether a Future has been successfully bound to the managed workflow item.

    The returned asyncio.Task can be used to cancel the rp.Task (and the Future)
    or to await the RP.Task cleanup.

    To submit tasks as a batch, await an array of submit() results in the
    same dispatching context. (TBD)
    """
    # TODO(#335): Decouple from scalems.radical.runtime.configuration. Use RuntimeManager or Executor interface.
    from scalems.radical.runtime import current_configuration

    config = current_configuration()
    if config is None:
        raise APIError("subprocess_to_rp_task() needs an active run time.")

    # TODO: Optimization: skip tasks that are already done (cached results available).
    def raptor_is_ready(raptor_id):
        return (
            isinstance(raptor_id, str) and len(raptor_id) > 0 and isinstance(task_manager.get_tasks(raptor_id), rp.Task)
        )

    subprocess_type = TypeIdentifier(("scalems", "subprocess", "SubprocessTask"))
    submitted_type = item.description().type()
    if submitted_type == subprocess_type:
        if raptor_id is not None:
            raise DispatchError("Raptor not yet supported for scalems.executable.")
        rp_task_description = _describe_legacy_task(item, pre_exec=pre_exec)
    elif config.enable_raptor:
        if raptor_is_ready(raptor_id):
            # We might want a contextvars.Context to hold the current rp.Master instance name.
            rp_task_description = _describe_raptor_task(item, raptor_id, pre_exec=pre_exec)
        else:
            raise APIError("Caller must provide the UID of a submitted *raptor* task.")
    else:
        raise APIError(f"Cannot dispatch {submitted_type}.")

    # TODO(#249): A utility function to move slow blocking RP calls to a separate thread.
    #  Compartmentalize TaskDescription -> rp_task_watcher in a separate utility function.
    (task,) = task_manager.submit_tasks([rp_task_description])

    rp_task_watcher = await rp_task(rptask=task)

    if rp_task_watcher.done():
        if rp_task_watcher.cancelled():
            raise DispatchError(f"Task for {item} was unexpectedly canceled during dispatching.")
        e = rp_task_watcher.exception()
        if e is not None:
            raise DispatchError("Task for {item} failed during dispatching.") from e

    # Warning: in the long run, we should not extend the life of the reference returned
    # by edit_item, and we need to consider the robust way to publish item results.
    # TODO: Translate RP result to item result type.
    rp_task_watcher.add_done_callback(functools.partial(scalems_callback, item=item))
    # TODO: If *item* acquires a `cancel` method, we need to subscribe to it and
    #  respond by unregistering the callback and canceling the future.

    return rp_task_watcher


def scalems_callback(fut: asyncio.Future, *, item: scalems.workflow.Task):
    """Propagate the results of a Future to the subscribed *item*.

    Partially bind *item* to use this as the argument to *fut.add_done_callback()*.

    Warning:
        In the long run, we should not extend the life of the reference returned
        by edit_item, and we need to consider the robust way to publish item results.

    Note:
        This is not currently RP-specific and we should look at how best to factor
        results publishing for workflow items. It may be that this function is the
        appropriate place to convert RP Task results to scalems results.
    """
    assert fut.done()
    if fut.cancelled():
        logger.info(f"Task supporting {item} has been cancelled.")
    else:
        # The following should not throw because we just checked for `done()` and
        # `cancelled()`
        e = fut.exception()
        if e:
            logger.info(f"Task supporting {item} failed: {e}")
        else:
            # TODO: Construct an appropriate scalems Result from the rp Task.
            item.set_result(fut.result())
