"""Support for scalems on radical.pilot.raptor.

Define the connective tissue for SCALE-MS tasks embedded in rp.Task arguments.

Provide the RP Raptor and Worker details. Implement the raptor task
script as a package entry point that we expect to be executable from the
command line in a virtual environment where the scalems package is installed,
without further modification to the PATH.

The client should be reasonably certain that the target environment has a
compatible installation of RP and scalems. A rp.raptor.Master task script is
installed with the scalems package. The script name is provide by the
module function :py:func:`~scalems.radical.raptor.raptor_script()`, and will
be resolvable on the PATH for a Python interpreter process in an
environment that has the `scalems` package installed.

We should try to keep this module as stable as possible so that the run time
interface provided by scalems to RP is robust, and the entry point scripts
change as little as possible across versions.
scalems.radical runtime details live in runtime.py.

As of :py:mod:`radical.pilot` version 1.18, TaskDescription supports
a *mode* field and several additional fields. The extended schema for the TaskDescription
depends on the value of *mode*. The structured data provided to the executor callable
is composed from these additional fields according to the particular mode.

We use the :py:data:`radical.pilot.task_description.TASK_FUNCTION` mode,
specifying a *function* field to name a callable in the (global) namespace
accessible by the worker process.

We populate the worker global namespace with imported callables in the module
file from which Raptor imports our :py:class:`radical.pilot.raptor.MPIWorker` subclass,
:py:class:`~scalems.radical.raptor.ScaleMSWorker`.

The callable for *call* accepts ``*args`` and ``**kwargs``, which are extracted
from fields in the TaskDescription. Since we derive from MPIWorker, an mpi4py
communicator is inserted at ``args[0]``.

Protocol
--------

As of RP 1.18, the :py:mod:`radical.pilot.raptor` interface is not documented.
The following diagrams illustrate the approximate architecture of a Raptor Session.

.. uml::

    title RP Raptor client Session

    box "RP client"
    participant TaskManager
    end box

    'queue "Taskmanager: Scheduler and Input Queue" as Queue
    queue "Scheduler/Input Queue" as Queue

    -> TaskManager: submit(master_task_description)
    activate TaskManager

    'note left
    note over TaskManager
     TaskDescription uses `mode=rp.RAPTOR_MASTER`.
     Task args provide details for the script
     so that a Raptor can start appropriate Workers.
    end note

    TaskManager -> Queue: task_description
    activate Queue
    deactivate TaskManager
    Queue <-
    Queue -->
    deactivate Queue

    -> TaskManager: submit(task_description)
    activate TaskManager

    'note left
    note over TaskManager
     TaskDescription names a Raptor uid in *raptor_id* field.
    end note

    TaskManager -> Queue: task_description
    activate Queue
    deactivate TaskManager
    Queue <-
    Queue -->
    deactivate Queue
    ...
    TaskManager ->
    TaskManager <-- : raptor task results

A "raptor task" is an *executable* task (*mode* = ``rp.RAPTOR_MASTER``)
in which the script named by
:py:data:`radical.pilot.TaskDescription.executable`
manages the life cycle of a :py:class:`radical.pilot.raptor.Master`
(or subclass instance).
As of RP 1.14, the protocol is as follows.

.. uml::

    title raptor raptor task lifetime management

    participant "raptor task"

    -> "raptor task" : stage in

    create Raptor as raptor
    "raptor task" -> raptor: create Raptor(cfg)
    "raptor task" -> raptor: Raptor.submit_workers(descr=descr, count=n_workers)
    "raptor task" -> raptor: Raptor.start()
    alt optional hook for self-submitting additional tasks
    "raptor task" -> raptor: Raptor.submit_tasks(tasks)
    end
    queue scheduler
    scheduler -\\ raptor : request_cb
    scheduler -\\ raptor : result_cb
    raptor -> raptor: Raptor.stop() (optional)
    "raptor task" -> raptor: Raptor.join()

    [<-- "raptor task" : stage out


The *cfg* argument to :py:class:`radical.pilot.raptor.Master` does not currently
appear to be required.

*scalems* encodes worker requirements on the client side. The *scalems* raptor
script decodes the client-provided requirements, combines the information with
run time details and work load inspection, and produces the WorkerDescription
with which to :py:func:`~radical.pilot.raptor.Master.submit_workers()`.

.. uml::

    class RaptorWorkerConfig {
     count
     descr
    }

    class WorkerDescription

    RaptorWorkerConfig *--> "descr" WorkerDescription

    namespace scalems.radical {
     class RaptorConfiguration

     class ClientWorkerRequirements

     class ScaleMSRaptor

     ScaleMSRaptor --> RaptorConfiguration : launches with

     RaptorConfiguration *-- ClientWorkerRequirements

     ClientWorkerRequirements -right-> .RaptorWorkerConfig : worker_description()
    }

    namespace radical.pilot.raptor {
     class Master {
      submit_workers()
     }
     Master::submit_workers .> .RaptorWorkerConfig
    }

    scalems.radical.ScaleMSRaptor -up-|> radical.pilot.raptor.Master

.. uml::

    title scalems raptor runtime management
    !pragma teoz true
    box "client"
    box "scalems.radical" #honeydew
    participant RPDispatchingExecutor as client_runtime
    end box

    box "radical.pilot" #White
    participant task_manager
    end box
    end box

    queue "RP runtime" as scheduler

    -> client_runtime : runtime_startup()
    activate client_runtime
    client_runtime -> client_runtime : coro_get_scheduler()
    activate client_runtime
    client_runtime -> client_runtime : raptor_script()
    client_runtime -> client_runtime : raptor_input()
    activate client_runtime
    client_runtime -> client_runtime : worker_requirements()
    activate client_runtime

    note over client_runtime
    TODO: Allocate Worker according to workload
     through a separate call to the running Raptor
     and remove ClientWorkerRequirements from RaptorConfiguration.
    end note

    return ClientWorkerRequirements
    return RaptorConfiguration

    client_runtime -> client_runtime: launch Raptor Task
    activate client_runtime

    note over client_runtime, task_manager
    We currently require a pre-existing venv for Raptor task.
    TODO(#90,#141): Allow separate venv for Worker task and Tasks.
    end note

    client_runtime -> task_manager : submit raptor task
    task_manager -\\ scheduler

    note over task_manager, scheduler
    Raptor cfg is staged with TaskDescription.
    end note

    deactivate client_runtime

    note over client_runtime, task_manager
    TODO(#92,#105): Make sure the Worker starts successfully!!!
    end note

    return raptor task
    return dispatching context


TODO
----
Pass input and output objects more efficiently.

Worker processes come and go,
and are not descended from Raptor processes (which may not even be on the same node),
so we can't generally pass objects by reference or even in shared memory segments.
However, we can use optimized ZMQ facilities for converting network messages
directly to/from memory buffers.

In other cases, we can use the scalems typing system to specialize certain types
for optimized (de)serialization from/to file-backed storage or streams.

See https://github.com/SCALE-MS/scale-ms/issues/106

TODO
----
Get a stronger specification of the RP Raptor interface.

See https://github.com/radical-cybertools/radical.pilot/issues/2731

"""
from __future__ import annotations

__all__ = (
    "ClientWorkerRequirements",
    "RaptorConfiguration",
    "raptor_input",
    "worker_requirements",
    "worker_description",
    "SoftwareCompatibilityError",
    "ScaleMSWorker",
    "ScaleMSRaptor",
    "ScalemsRaptorWorkItem",
    "WorkerDescription",
    "RaptorWorkerConfig",
)

import abc
import argparse
import asyncio
import contextlib
import dataclasses
import functools
import importlib
import importlib.metadata
import json
import os
import pathlib
import sys
import tempfile
import typing
import warnings
import weakref
import zlib
from contextlib import ContextDecorator
from importlib.machinery import ModuleSpec
from importlib.util import find_spec
from collections.abc import Generator

import packaging.version

from scalems.exceptions import DispatchError
from scalems.identifiers import EphemeralIdentifier
from scalems.store import FileStore

if typing.TYPE_CHECKING:
    try:
        from mpi4py.MPI import Comm
    except ImportError:
        Comm = None


# We import rp before `logging` to avoid warnings when rp monkey-patches the
# logging module. This `try` suite helps prevent auto-code-formatters from
# rearranging the import order of built-in versus third-party modules.
import radical.pilot as rp

import scalems.exceptions
import scalems.messages
import scalems.file as _file
import scalems.store as _store

# QUESTION: How should we approach logging? To what degree can/should we integrate
# with rp logging?
import logging

logger = logging.getLogger(__name__)
# Note that we have not restricted propagation or attached a handler, so the messages
# to `logger` will end up going to the stderr of the process that imported this module
# (e.g. the raptor task or the Worker task) and appearing in the corresponding
# Task.stderr. This is available on the client side in the case of the raptor task,
# but only to the Raptor itself for the worker task.
# We should probably either add a specific LogHandler or integrate with the
# RP logging and Component based log files.
# See also
# * https://github.com/SCALE-MS/scale-ms/discussions/261
# * https://github.com/SCALE-MS/scale-ms/issues/255


@dataclasses.dataclass(frozen=True)
class BackendVersion:
    """Identifying information for the computing backend."""

    name: str
    """Identifying name (presumably a module name)."""

    version: str
    """Implementation revision identifier as a PEP 440 compatible version string."""


backend_version = BackendVersion(name="scalems.radical.raptor", version="0.0.0")

# TODO: Where does this identifier belong?
api_name = "scalems_v0"
"""Key for dispatching raptor Requests.

We can use this to identify the schema used for SCALE-MS tasks encoded in
arguments to raptor :py:data:`~radical.pilot.TASK_FUNCTION` mode executor.
"""

CPI_MESSAGE = "scalems.cpi"
"""Flag for scalems messages to be treated as CPI calls.

Used in the :py:attr:`~radical.pilot.TaskDescription.mode` field to indicate that the
object should be handled through the SCALEMS Compute Provider Interface machinery.
"""


class CpiCommand(abc.ABC):
    _registry: typing.MutableMapping[str, typing.Type["CpiCommand"]] = weakref.WeakValueDictionary()

    @classmethod
    @abc.abstractmethod
    def launch(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        """Process the RP Task as a CPI Command.

        Called in ScaleMSRaptor.request_cb().
        """
        ...

    @classmethod
    @abc.abstractmethod
    def result_hook(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        """Called during Master.result_cb."""

    @typing.final
    @classmethod
    def get(cls, command: scalems.messages.Command):
        return CpiCommand._registry[command.__class__.__qualname__]

    @classmethod
    @abc.abstractmethod
    def command_class(cls) -> str:
        """The qualified name of the associated `scalems.messages` Command class."""
        ...

    def __init_subclass__(cls, **kwargs):
        if cls is not CpiCommand:
            CpiCommand._registry[cls.command_class()] = cls
        super().__init_subclass__(**kwargs)


class CpiStop(CpiCommand):
    """Provide implementation for StopCommand in RP Raptor.

    When a STOP is received, the Master will stop handling new compute tasks
    from the client. Tasks generated by other Tasks already in the queue will
    continue to be allowed until/unless a firmer "stop" command is received.
    """

    @classmethod
    def command_class(cls) -> str:
        return scalems.messages.StopCommand.__qualname__

    @classmethod
    def launch(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        logger.debug("CPI STOP issued.")
        # TODO: Cleaner shutdown.
        # * Mark the Raptor as "shutting down".
        # * Wait for pending tasks to complete and shut down Worker(s)
        # * Allow override for more aggressive shutdown, such as if additional STOP
        #   messages arrive while processing this one (already "shutting down").
        task["stderr"] = ""
        task["stdout"] = "CPI STOP received"
        task["exit_code"] = 0
        manager.cpi_finalize(task)

    @classmethod
    def result_hook(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        manager.stop()
        logger.debug(f"Finalized {str(task)}.")


class CpiHello(CpiCommand):
    """Provide implementation for HelloCommand in RP Raptor."""

    @classmethod
    def launch(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        logger.debug("CPI HELLO in progress.")
        task["stderr"] = ""
        task["exit_code"] = 0
        task["stdout"] = repr(backend_version)
        # TODO: scalems object encoding.
        task["return_value"] = dataclasses.asdict(backend_version)
        logger.debug("Finalizing...")
        manager.cpi_finalize(task)

    @classmethod
    def result_hook(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        logger.debug(f"Finalized {str(task)}.")

    @classmethod
    def command_class(cls) -> str:
        return scalems.messages.HelloCommand.__qualname__


class CpiAddItem(CpiCommand):
    """Add an item to the managed workflow record.

    Descriptions of Work with no dependencies will be immediately scheduled for
    execution (converted to RP tasks and submitted).

    TBD: Data objects, references to existing data, completed tasks.
    """

    @classmethod
    def launch(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        """Repackage a AddItem command as a rp.TaskDescription for submission to the Worker.

        Note that we are not describing the exact function call directly,
        but an encoded function call to be handled by the `run_in_worker`
        dispatching function. The arguments will be serialized
        together with the function code object into the
        *work_item* key word argument for the RP Task.

        The Master.request_cb() submits individual scalems tasks with this function.
        More complex work is deserialized and managed by ScaleMSRaptor using
        the `raptor_work_deserializer` function, first.

        See Also:
            `scalems.radical.raptor.ScaleMSWorker.run_in_worker()`
        """
        # TODO: If the Raptor is already "shutting down", reject new work from the client,
        #  but continue to allow dynamically generated Tasks from already-queued work until/unless
        #  a more aggressive shut down is ordered.
        # submit the task and return its task ID right away,
        # then allow its result to either just be used to
        #   * trigger dependent work,
        #   * only appear in the Raptor report, or
        #   * be available to follow-up LRO status checks.
        add_item = typing.cast(
            scalems.messages.AddItem, scalems.messages.Command.decode(task["description"]["metadata"])
        )
        encoded_item = add_item.encoded_item
        # We do not yet use a strongly specified object schema. Just accept a dict.
        item_dict = json.loads(encoded_item)
        # Decouple the serialization schema since it is not strongly specified or robust.
        work_item = ScalemsRaptorWorkItem(
            func=item_dict["func"],
            module=item_dict["module"],
            args=item_dict["args"],
            kwargs=item_dict["kwargs"],
            comm_arg_name=item_dict.get("comm_arg_name", None),
        )
        # TODO(#277): Convert abstract inputs to concrete values. (More management interface.)
        # TODO(#277): Check dependencies and runnability before submitting.

        fingerprint = zlib.crc32(json.dumps(work_item, sort_keys=True).encode("utf8"))
        # TODO: More robust fingerprint. Ref scalems.serialization and scalems.store
        # TODO: Check for duplicates.
        scalems_task_id = f"scalems-task-{fingerprint}"

        scalems_task_description = rp.TaskDescription(
            _RaptorTaskDescription(
                uid=scalems_task_id,
                # Note that (as of this writing) *executable* is required but unused for Raptor tasks.
                executable="scalems",
                raptor_id=manager.uid,
                # Note we could use metadata to encode additional info for ScaleMSRaptor.request_cb,
                # but it is not useful for execution of the rp.TASK_FUNCTION.
                metadata=None,
                mode=rp.TASK_FUNCTION,
                function="run_in_worker",
                args=[],
                kwargs={"work_item": work_item},
            )
        )
        # Note: There is no `Task` object returned from `Raptor.submit_tasks()`
        # `Task` objects are currently only available on the client side;
        # the agent side handles task dicts---which the raptor will receive through the *request_cb()*.
        manager.submit_tasks(scalems_task_description)
        logger.debug(f"Submitted {str(scalems_task_description)} in support of {str(task)}.")

        # Record task metadata and track.
        task["return_value"] = scalems_task_id
        task["stdout"] = scalems_task_id
        task["exit_code"] = 0
        manager.cpi_finalize(task)

    @classmethod
    def result_hook(cls, manager: ScaleMSRaptor, task: TaskDictionary):
        logger.debug(f"Finalized {str(task)}.")

    @classmethod
    def command_class(cls) -> str:
        return scalems.messages.AddItem.__qualname__


EncodableAsDict = typing.Mapping[str, "Encodable"]
EncodableAsList = typing.List["Encodable"]
Encodable = typing.Union[str, int, float, bool, None, EncodableAsDict, EncodableAsList]


# def object_decoder(obj: dict):
#     """Provide the object_hook callback for a JSONDecoder."""


@functools.singledispatch
def object_encoder(obj) -> Encodable:
    """Provide the *default* callback for JSONEncoder."""
    raise TypeError(f"No decoder for {obj.__class__.__qualname__}.")


# def get_decoder() -> json.JSONDecoder:
#     """Get a JSONDecoder instance extended for types in this module."""
#     decoder = json.JSONDecoder(object_hook=object_decoder)
#     return decoder
#
#
# def get_encoder() -> json.JSONEncoder:
#     """Get a JSONEncoder instance extended for types in this module."""
#     encoder = json.JSONEncoder(default=object_encoder)
#     return encoder


# @object_encoder.register
# def _(obj: rp.TaskDescription) -> dict:
#     return obj.as_dict()


@dataclasses.dataclass(frozen=True)
class ClientWorkerRequirements:
    """Client-side details to inform worker provisioning.

    This structure is part of the `scalems.radical.raptor.RaptorConfiguration`
    provided to the raptor script. The raptor script uses this information
    when calling `worker_description()`.

    Use the :py:func:`worker_requirements()` creation function for a stable interface.
    """

    cores_per_process: int
    """Number CPU cores reserved per process (or MPI rank) for threading or child processes."""

    cpu_processes: int
    """Number of ranks in the Worker MPI context."""

    gpus_per_rank: float = 0
    """GPUs per Worker rank."""

    pre_exec: tuple[str] = ()
    """Lines of shell expressions to be evaluated before launching the worker process."""

    named_env: typing.Optional[str] = None
    """A registered virtual environment known to the raptor manager.

    Warnings:
        Not tested. Support has been delayed indefinitely.
    """


class _RaptorConfigurationDict(typing.TypedDict):
    """A `RaptorConfiguration` encoded as a `dict`."""

    versioned_modules: typing.List[typing.Tuple[str, str]]


@dataclasses.dataclass
class RaptorConfiguration:
    """Input to the script responsible for the RP raptor.

    .. todo:: Check the schema for RP.
        See https://github.com/radical-cybertools/radical.pilot/issues/2731

    Produced on the client side with `raptor_input`. Deserialized in the
    raptor task (`raptor`) to get a `RaptorWorkerConfig`.
    """

    versioned_modules: typing.List[typing.Tuple[str, str]]
    """List of name, version specifier tuples for required modules."""

    @classmethod
    def from_dict(cls, obj: _RaptorConfigurationDict):
        """Decode from a native dict.

        Support deserialization, such as from a JSON file in the raptor task.
        """
        return cls(versioned_modules=list(obj["versioned_modules"]))


@object_encoder.register
def _(obj: RaptorConfiguration) -> dict:
    return dataclasses.asdict(obj)


async def raptor_input(
    *,
    filestore: _store.FileStore,
) -> _file.AbstractFileReference:
    """Provide the input file for a SCALE-MS Raptor script.

    The resulting configuration file is staged with the Raptor scheduler task and
    provided as a command line argument. Most information can be provided after the
    Raptor starts (through CPI commands), moving forward, but we retain this input
    mechanism for now.

    Produces a file containing a serialized `RaptorConfiguration`.

    Args:
        filestore: (local) FileStore that will manage the generated AbstractFileReference.
    """
    if not isinstance(filestore, _store.FileStore) or filestore.closed or not filestore.directory.exists():
        raise ValueError(f"{filestore} is not a usable FileStore.")

    # TODO(#141): Add additional dependencies that we can infer from the workflow.
    versioned_modules = (("mpi4py", "3.0.0"), ("scalems", scalems.__version__), ("radical.pilot", rp.version))

    configuration = RaptorConfiguration(versioned_modules=list(versioned_modules))

    # Make sure the temporary directory is on the same filesystem as the local workflow.
    tmp_base = filestore.directory
    # TODO(Python 3.10) Use `ignore_cleanup_errors=True`
    tmpdir_manager = tempfile.TemporaryDirectory(dir=tmp_base)
    with tmpdir_manager as tmpdir:
        # Serialize the configuration to a temporary file, then add it to the
        # FileStore to get a fingerprinted, tracked file.
        config_file_name = "raptor_scheduler_config.json"
        config_file_path = os.path.join(tmpdir, config_file_name)
        with open(config_file_path, "w") as fh:
            json.dump(configuration, fh, default=object_encoder, indent=2)
        file_description = _file.describe_file(config_file_path, mode="r")
        add_file_task = asyncio.create_task(
            _store.get_file_reference(file_description, filestore=filestore), name="get-config-file-reference"
        )
        await asyncio.wait((add_file_task,), return_when=asyncio.FIRST_EXCEPTION)
        assert add_file_task.done()
        try:
            tmpdir_manager.cleanup()
        except OSError:
            logger.exception(f"Errors occurred while cleaning up {tmpdir}.")
    if isinstance(add_file_task.exception(), scalems.exceptions.DuplicateKeyError):
        checksum = await asyncio.to_thread(file_description.fingerprint)
        key = checksum.hex()
        # Note: the DuplicateKeyError could theoretically be the result of a
        # pre-existing file with the same internal path but a different key, but
        # such a condition would probably represent an internal error (bug).
        # TODO: Migrate away from container behavior of FileStore.
        return filestore[key]
    else:
        return add_file_task.result()


def worker_requirements(
    *,
    pre_exec: typing.Iterable[str],
    ranks_per_worker: int,
    cores_per_rank: int = 1,
    gpus_per_rank: typing.SupportsFloat = 0,
) -> ClientWorkerRequirements:
    """Get the requirements for the work load, as known to the client."""
    workload_metadata = ClientWorkerRequirements(
        named_env=None,
        pre_exec=tuple(pre_exec),
        cores_per_process=cores_per_rank,
        cpu_processes=ranks_per_worker,
        gpus_per_rank=float(gpus_per_rank),
    )

    return workload_metadata


parser = argparse.ArgumentParser(description="Command line entry point for RP raptor task.")
parser.add_argument("file", type=str, help="Input file (JSON) for configuring ScaleMSRaptor instance.")


class coverage_file(ContextDecorator):
    """Decorator for a function to conditionally record to the named coverage data file."""

    _Coverage = None
    cov = None
    _decorated_function = ""

    def __init__(self, filename=".coverage", verbose=True):
        self.filename = filename
        self.verbose = verbose
        if os.getenv("COVERAGE_RUN") is not None or os.getenv("SCALEMS_COVERAGE") is not None:
            logger.info("... testing with coverage")
            try:
                from coverage import Coverage

                self._Coverage = Coverage
            except ImportError:
                warnings.warn("Code coverage reporting is not available.")
        else:
            logger.info("No coverage testing")

    def __call__(self, func):
        # Apply the decorator
        assert callable(func)
        self._decorated_function = func.__qualname__
        # Return the decorated function.
        return super().__call__(func)

    def __enter__(self):
        if self._Coverage is not None:
            current_pid = os.getpid()
            coverage_pid = os.environ.get("SCALEMS_COVERAGE_ACTIVE", None)
            if coverage_pid is not None:
                if int(coverage_pid) != current_pid:
                    logger.info(f"PID {current_pid} inheriting coverage from {coverage_pid}.")
                else:
                    logger.info(f"{self._decorated_function}: Coverage API is already active for {current_pid}.")
                    # warnings.warn(f'Coverage API is already active in {current_pid}. Data race may occur.')
                    # TODO: Check whether this is a problem since result_cb() may be reentrant.
            else:
                coverage_pid = current_pid
                os.environ["SCALEMS_COVERAGE_ACTIVE"] = str(current_pid)
                logger.info(f"Starting coverage for {coverage_pid} in {os.getcwd()}")
                filename = self.filename
                if not os.path.isabs(filename):
                    filename = os.path.join(".", "coverage_dir", filename)
                self.filename = os.path.abspath(filename)
                if not os.path.exists(os.path.dirname(filename)):
                    os.mkdir(os.path.dirname(filename))

                self.cov = self._Coverage(
                    auto_data=True,
                    branch=True,
                    check_preimported=True,
                    data_file=filename,
                    data_suffix=self._decorated_function,
                    messages=self.verbose,
                    source=("scalems",),
                )
                self.cov.start()
        return self

    def __exit__(self, *exc):
        if self.cov is not None:
            current_pid = os.getpid()
            coverage_pid_str = os.environ.get("SCALEMS_COVERAGE_ACTIVE", None)
            if coverage_pid_str == str(current_pid):
                logger.info(f"Saving coverage data for {self._decorated_function} to {self.filename}.")
                self.cov.stop()
                self.cov.save()
                del os.environ["SCALEMS_COVERAGE_ACTIVE"]
            else:
                logger.info(
                    f"Deferring coverage for {self._decorated_function} on {current_pid} to PID {coverage_pid_str}."
                )
            del self.cov
        return False


def raptor():
    """Entry point for raptor.Master task.

    This function implements the :command:`scalems_raptor` entry point script
    called by the RADICAL Pilot executor to provide the raptor raptor task.

    During installation, the `scalems` build system creates a :file:`scalems_raptor`
    `entry point <https://setuptools.pypa.io/en/latest/pkg_resources.html#entry-points>`__
    script that provides command line access to this function. The Python signature
    takes no arguments because the function processes command line arguments from `sys.argv`

    .. uml::

        title scalems raptor raptor task lifetime
        !pragma teoz true

        queue "RP runtime" as scheduler

        box execution
        box "scalems.radical" #honeydew
        participant "raptor task"
        participant ScaleMSRaptor as raptor
        end box
        participant worker.py
        end box

        scheduler -> "raptor task" : stage in
        ?-> "raptor task" : scalems.radical.raptor.~__main__()
        activate "raptor task"

        note over "raptor task" : "TODO: Get asyncio event loop?"
        note over "raptor task" : "TODO: Initialize scalems FileStore"

        "raptor task" -> "raptor task" : from_dict
        activate "raptor task"
        return RaptorConfiguration
        "raptor task" -> raptor **: create ScaleMSRaptor()
        "raptor task" -> raptor: start()

        ...
        group CPI [launch workers]
            scheduler -\\ raptor : request_cb
            activate raptor

            raptor -> raptor : with configure_worker()
            activate raptor
            raptor -> worker.py ** : with _worker_file()
            activate raptor
            raptor -> raptor : _configure_worker()
            activate raptor
            raptor -> raptor : worker_description()
            activate raptor
            return
            return WorkerDescription
            deactivate raptor
            return RaptorWorkerConfig
            'raptor --> raptor : RaptorWorkerConfig
            'deactivate raptor

            raptor -> raptor: submit_workers(**config)
            activate raptor
            raptor -\\ worker.py
            activate worker.py
            raptor --> raptor: UIDs
            deactivate raptor
            raptor -> raptor: wait_workers()
            activate raptor
            return
            return
            deactivate raptor

        scheduler -\\ raptor : result_cb
        end
        ...

        group TODO [stop workers]
            "raptor task" -> raptor : ~__exit__
            activate raptor
            raptor -> raptor : ~__exit__
            activate raptor
            raptor --> raptor : stop worker (TODO)
            raptor -> worker.py !! : delete
            deactivate raptor
            raptor -> raptor: stop() (TBD)
        end

        "raptor task" -> raptor: join()
        deactivate raptor
        deactivate "raptor task"

        scheduler <-- "raptor task" : stage out
        deactivate "raptor task"

    """
    # TODO: Configurable log level?
    logger.setLevel(logging.DEBUG)
    character_stream = logging.StreamHandler()
    character_stream.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    character_stream.setFormatter(formatter)
    logger.addHandler(character_stream)

    file_logger = logging.FileHandler("scalems.radical.raptor.log")
    file_logger.setLevel(logging.DEBUG)
    file_logger.setFormatter(formatter)
    logging.getLogger("scalems").addHandler(file_logger)

    if not os.environ["RP_TASK_ID"]:
        warnings.warn("Attempting to start a Raptor without RP execution environment.")

    rp_version = packaging.version.parse(rp.version)

    args = parser.parse_args()
    if not os.path.exists(args.file):
        raise RuntimeError(f"File not found: {args.file}")
    with open(args.file, "r") as fh:
        configuration = RaptorConfiguration.from_dict(json.load(fh))

    logger.info(f"Launching ScaleMSRaptor task in raptor {rp_version} with {repr(dataclasses.asdict(configuration))}.")

    _raptor = ScaleMSRaptor(configuration)
    logger.debug(f"Created {repr(_raptor)}.")

    try:
        _raptor.start()
        logger.debug(f"Raptor started in PID {os.getpid()}: {' '.join(sys.argv)}.")

        # As of RP 1.33, `join` waits on a thread that monitors Worker responsiveness until
        # a termination Event is detected.
        _raptor.join()
        logger.debug("Raptor joined.")
    finally:
        if sys.exc_info() != (None, None, None):
            logger.exception("Raptor task encountered exception.")
        logger.debug("Completed raptor task.")


def _configure_worker(*, requirements: ClientWorkerRequirements, filename: str) -> RaptorWorkerConfig:
    """Prepare the arguments for :py:func:`rp.raptor.Master.submit_workers()`.

    Provide an abstraction for the submit_workers signature and parameter types,
    which may still be evolving.

    See also https://github.com/radical-cybertools/radical.pilot/issues/2731
    """
    assert os.path.exists(filename)
    # TODO(#248): Consider a "debug-mode" option to do a trial import from *filename*
    # TODO(#302): Number of workers should be chosen after inspecting the requirements of the work load.
    num_workers = 1
    descr = worker_description(
        named_env=requirements.named_env,
        worker_file=filename,
        pre_exec=requirements.pre_exec,
        cpu_processes=requirements.cpu_processes,
        gpus_per_rank=requirements.gpus_per_rank,
    )
    if os.getenv("COVERAGE_RUN") is not None or os.getenv("SCALEMS_COVERAGE") is not None:
        descr.environment = {"SCALEMS_COVERAGE": "TRUE"}
    config: RaptorWorkerConfig = [descr] * num_workers
    return config


class SoftwareCompatibilityError(RuntimeError):
    """Incompatible package versions or software interfaces."""


def _get_module_version(module: str):
    """Get version metadata for importable module."""
    try:
        found_version = importlib.metadata.version(module)
    except importlib.metadata.PackageNotFoundError:
        found_version = None
    if found_version is None:
        try:
            spec: ModuleSpec = find_spec(module)
            if spec and hasattr(spec, "parent"):
                found_version = importlib.metadata.version(spec.parent)
        except Exception as e:
            logger.debug(f"Exception when trying to find {module}: ", exc_info=e)
    if found_version is not None:
        found_version = packaging.version.Version(found_version)
    return found_version


class ScaleMSRaptor(rp.raptor.Master):
    """Manage a RP Raptor *raptor_id* target.

    Extends :py:class:`rp.raptor.Master`.

    We do not submit scalems tasks directly as RP Tasks from the client.
    We encode scalems tasks as instructions to the Raptor task, which will be
    decoded and translated to RP Tasks by the `ScaleMSRaptor.request_cb` and
    self-submitted to the `ScaleMSWorker`.

    Results of such tasks are only available through to the :py:func:`result_cb`.
    The Raptor can translate results of generated Tasks into results for the Task
    carrying the coded instruction, or it can produce data files to stage during or
    after the Raptor task. Additionally, the Raptor could respond to other special
    instructions (encoded in later client-originated Tasks) to query or retrieve
    generated task results.

    .. uml::

        title Raptor

        queue "Queue" as Queue

        box "RP Agent"
        participant Scheduler
        end box

        queue "Raptor input queue" as master_queue

        box "scalems.radical.raptor"
        participant ScaleMSRaptor
        participant rpt.Master
        end box

        queue "ZMQ Raptor work channel" as channel

        activate Queue

        Scheduler -> Queue: accepts work
        activate Scheduler
        Scheduler <-- Queue: task dictionary
        deactivate Queue

        note over Scheduler
        Scheduler gets task from queue,
        observes `scheduler` field and routes to Raptor.
        end note

        Scheduler -> master_queue: task dictionary
        activate master_queue
        deactivate Scheduler

        note over ScaleMSRaptor, rpt.Master
        Back end RP queue manager
        passes messages to Raptor callbacks.
        end note

        rpt.Master -> master_queue: accept Task
        activate rpt.Master
        rpt.Master <-- master_queue: cpi_task
        deactivate master_queue

        rpt.Master -> rpt.Master: _request_cb(cpi_task)
        activate rpt.Master
        rpt.Master -> ScaleMSRaptor: request_cb(cpi_task)
        activate ScaleMSRaptor

        ScaleMSRaptor -> ScaleMSRaptor: CpiCommand.launch()
        activate ScaleMSRaptor
        alt optionally process or update requests
          ScaleMSRaptor -> ScaleMSRaptor: submit_tasks(scalems_tasks)
          activate ScaleMSRaptor
          deactivate ScaleMSRaptor
          ScaleMSRaptor -> ScaleMSRaptor: _result_cb(cpi_task)
          activate ScaleMSRaptor
          deactivate ScaleMSRaptor
        else resolve a Control message
          ScaleMSRaptor -> ScaleMSRaptor: _result_cb(cpi_task)
          activate ScaleMSRaptor
          deactivate ScaleMSRaptor
        end
        return

        return filtered tasks

        rpt.Master -> rpt.Master: submit_tasks(...)
        activate rpt.Master
        deactivate rpt.Master
        rpt.Master -> channel: send message
        deactivate rpt.Master
        deactivate rpt.Master

        rpt.Master -> channel: accept result
        activate rpt.Master
        rpt.Master <-- channel: scalems_tasks
        deactivate channel

        rpt.Master -> rpt.Master: _result_cb(scalems_tasks))
        activate rpt.Master
        rpt.Master -> ScaleMSRaptor: result_cb(scalems_tasks)
        activate ScaleMSRaptor

        ScaleMSRaptor -> ScaleMSRaptor: CpiCommand.result_hook()

        alt optionally process or update pending requests
        ScaleMSRaptor -> ScaleMSRaptor: issue#277
        activate ScaleMSRaptor
        deactivate ScaleMSRaptor
        end

        rpt.Master <-- ScaleMSRaptor
        deactivate ScaleMSRaptor

        rpt.Master -> master_queue: send message
        activate master_queue
        deactivate rpt.Master
        deactivate rpt.Master

    """

    def __init__(self, configuration: RaptorConfiguration):
        """Initialize a SCALE-MS Raptor.

        Verify the environment. Perform some scalems-specific set up and
        initialize the base class details.
        """
        # Verify environment.
        for module, version in configuration.versioned_modules:
            logger.debug(f"Looking for {module} version {version}.")
            found_version = _get_module_version(module)
            if found_version is None:
                raise SoftwareCompatibilityError(f"{module} not found.")
            minimum_version = packaging.version.Version(version)
            if found_version >= minimum_version:
                logger.debug(f"Found {module} version {found_version}: Okay.")
            else:
                raise SoftwareCompatibilityError(f"{module} version {found_version} not compatible with {version}.")
        # The rp.raptor base class will fail to initialize without the environment
        # expected for a RP task launched by an RP agent. However, we may still
        # want to acquire a partially-initialized instance for testing.
        try:
            super(ScaleMSRaptor, self).__init__()
        except KeyError:
            warnings.warn("Raptor incomplete. Could not initialize raptor.Master base class.")

        # Initialize internal state.
        # TODO: Use a scalems RuntimeManager.
        self.__worker_files = {}

    @contextlib.contextmanager
    def configure_worker(self, requirements: ClientWorkerRequirements) -> Generator[RaptorWorkerConfig, None, None]:
        """Scoped temporary module file for raptor worker.

        Write and return the path to a temporary Python module. The module imports
        :py:class:`ScaleMSWorker` into its module namespace
        so that the file and class can be used in the worker description for
        :py:func:`rp.raptor.Master.submit_workers()`
        """
        with self._worker_file() as worker_file:
            assert os.path.abspath(worker_file)
            assert os.path.exists(worker_file)
            worker_config = _configure_worker(requirements=requirements, filename=worker_file)
            yield worker_config

    @contextlib.contextmanager
    def _worker_file(self):
        """Scoped temporary module file for raptor worker.

        Not thread-safe. (Should it be?)

        Write and return the path to a temporary Python module. The module imports
        :py:class:`ScaleMSWorker` into its module namespace
        so that the file and class can be used in the worker description for
        :py:func:`rp.raptor.Master.submit_workers()`
        """
        worker = ScaleMSWorker
        # Note: there does not appear to be any utility in any lines to evaluate
        # beyond the Worker subclass import. This namespace will not be available
        # (through `globals()` or `locals()`, at least) to the rp.TASK_FUNCTION
        # implementation in rp.raptor.worker_mpi._Worker._dispatch_function.
        text = [
            f"from {worker.__module__} import {worker.__name__}\n",
            # f'from {run_in_worker.__module__} import {run_in_worker.__name__}\n'
        ]
        # TODO: Use the Raptor's FileStore to get an appropriate fast shared filesystem.
        with tempfile.TemporaryDirectory() as tmp_dir:
            filename = self.__worker_files.get(worker, None)
            if filename is None:
                filename = next_numbered_file(dir=tmp_dir, name="scalems_worker", suffix=".py")
                with open(filename, "w") as fh:
                    fh.writelines(text)
                self.__worker_files[worker] = filename
            yield filename
        del self.__worker_files[worker]

    def request_cb(self, tasks: typing.Sequence[TaskDictionary]) -> typing.Sequence[TaskDictionary]:
        """Allows all incoming requests to be processed by the Raptor.

        RADICAL guarantees that :py:func:`~radical.pilot.raptor.Master.request_cb()`
        calls are made sequentially in a single thread.
        The implementation does not need to be thread-safe or reentrant.
        (Ref: https://github.com/radical-cybertools/radical.utils/blob/master/src/radical/utils/zmq/queue.py#L386)

        RADICAL does not guarantee that Tasks are processed in the same order in
        which they are submitted by the client.

        If overridden, request_cb() must return a :py:class:`list`. The returned list
        is interpreted to be requests that should be processed normally after the callback.
        This allows subclasses of rp.raptor.Master to add or remove requests before they become Tasks.
        The returned list is submitted with self.submit_tasks() by the base class after the
        callback returns.

        A Raptor may call *self.submit_tasks()* to self-submit items (e.g. instead of or in
        addition to manipulating the returned list in *request_cb()*).

        It is the developer's responsibility to choose unique task IDs (uid) when crafting
        items for Raptor.submit_tasks().
        """
        try:
            # It is convenient to compartmentalize the filtering and dispatching
            # of scalems tasks in a generator function, but this callback signature
            # requires a `list` to be returned.
            remaining_tasks = self._scalems_handle_requests(tasks)
            # Let non-scalems tasks percolate to the default machinery.
            return list(remaining_tasks)
        except Exception as e:
            logger.exception("scalems request filter propagated an exception.")
            # TODO: Submit a clean-up task.
            #  Use a thread or asyncio event loop controlled by the main raptor task thread
            #  to issue a cancel to all outstanding tasks and stop the workers.
            # Question: Is there a better / more appropriate way to exit cleanly on errors?
            self.stop()
            # Note that we are probably being called in a non-root thread.
            # TODO: Avoid letting an exception escape unhandled.
            # WARNING: RP 1.18 suppresses exceptions from request_cb(), but there is a note
            # indicating that exceptions will cause task failure in a future version.
            # But the failure modes of the scalems raptor task are not yet well-defined.
            # See also
            # * https://github.com/SCALE-MS/scale-ms/issues/218 and
            # * https://github.com/SCALE-MS/scale-ms/issues/229
            raise e

    def cpi_finalize(self, task: TaskDictionary):
        """Short-circuit the normal Raptor protocol to finalize a task.

        This is an alias for Raptor._result_cb(), but is a public method used
        in the :py:class:`CpiCommand.launch` method for
        :py:class:`~scalems.messages.Control` commands, which do not
        call :py:func:`ScaleMSRaptor.submit_tasks`.
        """
        self._result_cb(task)

    def _scalems_handle_requests(self, tasks: typing.Sequence[TaskDictionary]):
        for task in tasks:
            _finalized = False
            try:
                mode = task["description"]["mode"]
                # Allow non-scalems work to be handled normally.
                if mode != CPI_MESSAGE:
                    logger.debug(f"Deferring {str(task)} to regular RP handling.")
                    yield task
                    continue
                else:
                    command = scalems.messages.Command.decode(task["description"]["metadata"])
                    logger.debug(f"Received message {command} in {str(task)}")

                    impl: typing.Type[CpiCommand] = CpiCommand.get(command)

                    # # TODO(#277)
                    # # Check work dependencies. Generate necessary data staging.
                    # self._dependents[task['uid']] = ...
                    # # Generate sub-tasks and manage dependencies. Prune tasks that
                    # # are already completed. Produce appropriate error if work cannot
                    # # be performed. Manage sufficient state that result_cb is able to
                    # # submit tasks as their dependencies are met.
                    #
                    # self._workload.add(task)
                    # ...

                    impl.launch(self, task)
                    # Note: this _finalized flag is immediately useless for non-Control commands.
                    # TODO: We need more task state maintenance, probably with thread-safety.
                    _finalized = True
            except Exception as e:
                # Exceptions here are presumably bugs in scalems or RP.
                # Almost certainly, we should shut down after cleaning up.
                # 1. TODO: Record error in log for ScaleMSRaptor.
                # 2. Make sure that task is resolved.
                # 3. Trigger clean shut down of raptor task.
                if not _finalized:
                    # TODO: Mark failed, or note exception
                    self._result_cb(task)
                raise e

    def result_cb(self, tasks: typing.Sequence[TaskDictionary]):
        """SCALE-MS specific handling of completed tasks.

        We perform special handling for two types of tasks.
        1. Tasks submitted throughcompleted by the collaborating Worker(s).
        2. Tasks intercepted and resolved entirely by the Raptor (CPI calls).
        """
        # Note: At least as of RP 1.18, exceptions from result_cb() are suppressed.
        for task in tasks:
            logger.debug(f'Result callback for {task["description"]["mode"]} task {task["uid"]}: {task}')
            mode = task["description"]["mode"]
            # Allow non-scalems work to be handled normally.
            if mode == CPI_MESSAGE:
                command = scalems.messages.Command.decode(task["description"]["metadata"])
                logger.debug(f"Finalizing {command} in {str(task)}")

                impl: typing.Type[CpiCommand] = CpiCommand.get(command)
                impl.result_hook(self, task)

            # # Release dependent tasks.
            # #  https://github.com/SCALE-MS/randowtal/blob/c58452a3eaf0c058337a85f4fca3f51c5983a539/test_am
            # #  /brer_master.py#L114
            # # TODO: use scalems work graph management.
            # if task['uid'] in self._dependents:
            #     # Warning: The dependent task may have multiple dependencies.
            #     dep = self._dependents[task['uid']]
            #     self._log.debug('=== submit dep  %s', dep['uid'])
            #     self.submit_tasks(dep)
            #
            # mode = task['description']['mode']
            #
            # # NOTE: `state` will be `AGENT_EXECUTING`
            # self._log.debug('=== out: %s', task['stdout'])
            # self._log.debug('result_cb  %s: %s [%s] [%s]',
            #                 task['uid'],
            #                 task['state'],
            #                 sorted(task['stdout']),
            #                 task['return_value'])

            # #TODO(#108)
            # # Complete and publish
            # #  https://github.com/SCALE-MS/randowtal/blob/c58452a3eaf0c058337a85f4fca3f51c5983a539/test_am
            # #  /brer_master.py#L114
            # # Check whether all of the submitted tasks have been completed.
            # if self._submitted == self._completed:
            #     # we are done, not more workloads to wait for - return the tasks
            #     # and terminate
            #     # TODO: We can advance the rp task state with a callback registered with the corresponding
            #     #  unwrapped scalems task.
            #     for req in self._workload:
            #         # TODO: create raptor method
            #         req['task']['target_state'] = rp.DONE
            #         self.advance(req['task'], rp.AGENT_STAGING_OUTPUT_PENDING,
            #                      publish=True, push=True)


class WorkerDescription(rp.TaskDescription):
    """Worker description.

    See Also:
        * :py:meth:`~radical.pilot.raptor.Master.submit_workers()`
        * https://github.com/radical-cybertools/radical.pilot/issues/2731

    """

    cores_per_rank: typing.Optional[int]
    environment: typing.Optional[dict[str, str]]
    gpus_per_rank: typing.Optional[float]
    named_env: typing.Optional[str]
    pre_exec: typing.Optional[list[str]]
    ranks: int
    raptor_class: str
    raptor_file: str
    mode: str  # rp.RAPTOR_WORKER


RaptorWorkerConfig = list[WorkerDescription]
"""Client-specified Worker requirements.

Used by raptor script when calling `worker_description()`.
Created internally with :py:func:`_configure_worker()`.

See Also:
    :py:mod:`scalems.radical.raptor.ClientWorkerRequirements`

"""


def worker_description(
    *,
    named_env: typing.Optional[str],
    worker_file: str,
    cores_per_process: int = None,
    cpu_processes: int = None,
    gpus_per_rank: float = None,
    pre_exec: typing.Iterable[str] = (),
    environment: typing.Optional[typing.Mapping[str, str]] = None,
) -> WorkerDescription:
    """Get a worker description for ScaleMSRaptor.submit_workers().

    Parameters:
        cores_per_process (int, optional): See `radical.pilot.TaskDescription.cores_per_rank`
        cpu_processes (int, optional): See `radical.pilot.TaskDescription.ranks`
        environment (dict, optional): Environment variables to set in the Worker task.
        gpus_per_rank (float, optional): See `radical.pilot.TaskDescription.gpus_per_rank`
        named_env (str): Python virtual environment registered with `radical.pilot.Pilot.prepare_env`
            (currently ignored. see #90).
        pre_exec (list[str]): Shell command lines for preparing the worker environment.
        worker_file (str): Standalone module from which to import ScaleMSWorker.

    *worker_file* is generated for the raptor script by the caller.
    See :py:mod:`scalems.radical.raptor`

    The *uid* for the Worker task is defined by the ScaleMSRaptor.submit_workers().
    """
    descr = WorkerDescription()
    if cores_per_process is not None:
        descr.cores_per_rank = cores_per_process
    if environment is not None:
        descr.environment = environment
    if gpus_per_rank is not None:
        descr.gpus_per_rank = gpus_per_rank
    if named_env is not None:
        # descr.named_env=None
        logger.warning(f"Ignoring named_env={named_env}. Parameter is currently unused.")
    descr.pre_exec = list(pre_exec)
    if cpu_processes is not None:
        descr.ranks = cpu_processes
    descr.raptor_class = "ScaleMSWorker"
    descr.raptor_file = worker_file
    descr.mode = rp.RAPTOR_WORKER
    return descr


class RaptorTaskExecutor(typing.Protocol):
    """Represent the signature of executor functions for rp.raptor.MPIWorker."""

    def __call__(self, *args, comm: Comm, **kwargs) -> Encodable:
        ...


class ScaleMSWorker(rp.raptor.MPIWorker):
    """Specialize the Raptor MPI Worker for scalems dispatching of serialised work.

    scalems tasks encode the importable function and inputs in the arguments to
    the `run_in_worker` dispatching function,
    which is available to the `ScaleMSWorker` instance.

    .. uml::

        title ScaleMSWorker task dispatching

        queue "ZMQ Raptor work channel" as channel

        box "scalems.radical.raptor.Worker"
        participant ScaleMSWorker
        participant rpt.Worker
        end box

        box "usermodule"
        participant usermodule
        end box

        box "target venv"
        end box

        rpt.Worker -> channel: pop message
        activate rpt.Worker
        rpt.Worker <-- channel: rp.TASK_FUNCTION mode Task
        rpt.Worker -> rpt.Worker: _dispatch_function()
        activate rpt.Worker

        alt scalems encoded workload
          rpt.Worker -> ScaleMSWorker: run_in_worker()
          activate ScaleMSWorker
          ScaleMSWorker -> ScaleMSWorker: unpack encoded function call
          ScaleMSWorker -> ScaleMSWorker: from usermodule import func
          ScaleMSWorker -> usermodule: ""func(*args, **kwargs)""
          activate usermodule
          ScaleMSWorker <-- usermodule
          deactivate usermodule
          return
        else pickled rp.PythonTask workload (not implemented)
          rpt.Worker -> rpt.Worker: unpickled scalems handler
          activate rpt.Worker
          rpt.Worker -> usermodule: ""func(*args, **kwargs)""
          activate usermodule
          rpt.Worker <-- usermodule
          deactivate usermodule
          rpt.Worker --> rpt.Worker: {out, err, ret, value}
          deactivate rpt.Worker
        end

        deactivate rpt.Worker
        rpt.Worker -> channel: put result
        deactivate rpt.Worker

    """

    # TODO: How to get the module logger output for the Worker task.
    @coverage_file()
    def run_in_worker(self, *, work_item: ScalemsRaptorWorkItem, comm=None):
        """Unpack and run a task requested through RP Raptor.

        Satisfies RaptorTaskExecutor protocol. Named as the *function* for
        rp.TASK_FUNCTION mode tasks generated by scalems.

        This function MUST be imported and referentiable by its *__qualname__* from
        the scope in which ScaleMSWorker methods execute. (See ScaleMSRaptor._worker_file)

        Parameters:
            comm (mpi4py.MPI.Comm, optional): MPI communicator to be used for the task.
            work_item: dictionary of encoded function call from *kwargs* in the TaskDescription.

        See Also:
            :py:func:`CpiAddItem.launch()`
        """
        module = importlib.import_module(work_item["module"])
        func = getattr(module, work_item["func"])
        args = list(work_item["args"])
        kwargs = work_item["kwargs"].copy()
        comm_arg_name = work_item.get("comm_arg_name", None)
        if comm_arg_name is not None:
            if comm_arg_name:
                kwargs[comm_arg_name] = comm
            else:
                args.append(comm)
        self._log.debug(
            "Calling {func} with args {args} and kwargs {kwargs}",
            {"func": func.__qualname__, "args": repr(args), "kwargs": repr(kwargs)},
        )
        return func(*args, **kwargs)


def raptor_work_deserializer(*args, **kwargs):
    """Unpack a workflow document from a Task.

    When a `ScaleMSRaptor.request_cb()` receives a raptor task, it uses this
    function to unpack the instructions and construct a work load.
    """
    raise scalems.exceptions.MissingImplementationError


class ScalemsRaptorWorkItem(typing.TypedDict):
    """Encode the function call to implement a task in the workflow.

    Parameter type for the `run_in_worker` function *work_item* argument.

    This structure must be trivially serializable (by `msgpack`) so that it can
    be passed in a TaskDescription.kwargs field. Consistency with higher level
    `scalems.serialization` is not essential, but can be pursued in the future.

    The essential role is to represent an importable callable and its arguments.
    At run time, the dispatching function (`run_in_worker`) conceivably has access
    to module scoped state, but does not have direct access to the Worker (or
    any resources other than the `mpi4py.MPI.Comm` object). Therefore, abstract
    references to input data should be resolved at the Raptor in terms of the
    expected Worker environment before preparing and submitting the Task.

    TODO: Evolve this to something sufficiently general to be a scalems.WorkItem.

    TODO: Clarify the translation from an abstract representation of work in terms
     of the workflow record to the concrete representation with literal values and
     local filesystem paths.

    TODO: Record extra details like implicit filesystem interactions,
     environment variables, etc. TBD whether they belong in a separate object.
    """

    func: str
    """A callable to be retrieved as an attribute in *module*."""

    module: str
    """The qualified name of a module importable by the Worker."""

    args: list
    """Positional arguments for *func*."""

    kwargs: dict
    """Key word arguments for *func*."""

    comm_arg_name: typing.Optional[str]
    """Identify how to provide an MPI communicator to *func*, if at all.

    If *comm_arg_name* is not None, the callable will be provided with the
    MPI communicator. If *comm_arg_name* is an empty string, the communicator
    is provided as the first positional argument. Otherwise, *comm_arg_name*
    must be a valid key word argument by which to pass the communicator to *func*.
    """


class _RaptorTaskDescription(typing.TypedDict):
    """Describe a Task to be executed through a Raptor Worker.

    A specialization of `radical.pilot.TaskDescription`.

    Note the distinctions of a TaskDescription to be processed by a raptor.Master.

    The meaning or utility of some fields is dependent on the values of other fields.

    TODO: We need some additional fields, like *environment* and fields related
     to launch method and resources reservation. Is the schema for rp.TaskDescription
     sufficiently strong and well-documented that we can remove this hinting type?
     (Check again at RP >= 1.19)
    """

    uid: str
    """Unique identifier for the Task across the Session."""

    executable: str
    """Unused by Raptor tasks."""

    raptor_id: str
    """The UID of the raptor.Master scheduler task.

    This field is relevant to tasks routed from client TaskManagers. It is not
    used for tasks originating in raptor tasks.

    .. ref https://github.com/SCALE-MS/scale-ms/discussions/258#discussioncomment-4087870

    """

    metadata: Encodable
    """An arbitrary user-provided payload.

    May be any type that is encodable by :py:mod:`msgpack` (i.e. built-in types).
    """

    mode: str
    """The executor mode for the Worker to use.

    For ``rp.TASK_FUNCTION`` mode, either *function* or *method* must name a task executor.
    Depending on the Worker (sub)class, resources such as an `mpi4py.MPI.Comm`
    will be provided as the first positional argument to the executor.
    ``*args`` and ``**kwargs`` will be provided to the executor from the corresponding
    fields.

    See Also:
        * :py:data:`CPI_MESSAGE`
        * :py:class:`RaptorTaskExecutor`
    """

    function: str
    """Executor for ``rp.TASK_FUNCTION`` mode.

    Names the callable for dispatching.

    The callable can either be a function object present in the namespace of the interpreter launched
    by the Worker for the task, or a :py:class:`radical.pilot.pytask.PythonTask` pickled function object.
    """

    args: list
    """For ``rp.TASK_FUNCTION`` mode, list of positional arguments provided to the executor function."""

    kwargs: dict
    """For ``rp.TASK_FUNCTION`` mode, a dictionary of key word arguments provided to the executor function."""


class TaskDictionary(typing.TypedDict):
    """Task representations seen by *request_cb* and *result_cb*.

    Other fields may be present, but the objects in the sequences provided to
    :py:meth:`scalems.radical.raptor.ScaleMSRaptor.request_cb()` and
    :py:meth:`scalems.radical.raptor.ScaleMSRaptor.result_cb()` have the following fields.
    Result fields will not be populated until the Task runs.

    For the expected fields, see the source code for
    :py:meth:`~radical.pilot.Task.as_dict()`:
    https://radicalpilot.readthedocs.io/en/stable/_modules/radical/pilot/task.html#Task.as_dict
    """

    uid: str
    """Canonical identifier for the Task.

    Note that *uid* may be omitted from the original TaskDescription.
    """

    description: _RaptorTaskDescription
    """Encoding of the original task description."""

    stdout: str
    """Task standard output."""

    stderr: str
    """Task standard error."""

    exit_code: int
    """Task return code."""

    return_value: typing.Any
    """Function return value.

    Refer to the :py:class:`RaptorTaskExecutor` Protocol.
    """

    exception: str
    """Exception type name and message."""

    exception_detail: str
    """Full exception details with stack trace."""

    state: str
    """RADICAL Pilot Task state."""

    target_state: str
    """State to which the Task should be advanced.

    Valid values are string constants from :py:mod:`radical.pilot.states`.

    Used internally, such as in Master._result_cb().
    """


def next_numbered_file(*, dir, name, suffix):
    """Get the next available filename with the form {dir}/{name}{i}{suffix}."""
    if not os.path.exists(dir):
        raise ValueError(f'Directory "{dir}" does not exist.')
    filelist = os.listdir(dir)
    template = str(name) + "{i}" + str(suffix)
    i = 1
    filename = template.format(i=i)
    while filename in filelist:
        i += 1
        filename = template.format(i=i)
    return os.path.join(dir, filename)


async def coro_get_scheduler(
    pre_exec: typing.Iterable[str],
    pilot: rp.Pilot,
    filestore: FileStore,
    config_future: asyncio.Future[_file.AbstractFileReference],
) -> rp.raptor_tasks.Raptor:
    """Establish the radical.pilot.raptor.Master task.

    Create a raptor rp.Task (running the scalems_rp_master script) with the
    provided *name* to be referenced as the *scheduler* for raptor tasks.

    Returns the rp.Task for the raptor script once the Master is ready to
    receive submissions.

    Raises:
        DispatchError if the raptor task could not be launched successfully.

    Note:
        Currently there is no completion condition for the raptor script.
        Caller is responsible for canceling the Task returned by this function.
    """
    # define a raptor.scalems raptor and launch it within the pilot
    td = rp.TaskDescription()

    # The raptor uid is used as the `scheduler` value for raptor task routing.
    # TODO(#108): Use caller-provided *name* for master_identity.
    # Master tasks may not appear unique, but must be uniquely identified within the
    # scope of a rp.Session for RP bookkeeping. Since there is no other interesting
    # information at this time, we can generate a random ID and track it in our metadata.
    master_identity = EphemeralIdentifier()
    td.uid = "scalems-rp-raptor." + str(master_identity)

    td.mode = rp.RAPTOR_MASTER

    # scalems_rp_master will write output before it begins handling requests. The
    # script may crash even before it can write anything, but if it does write
    # anything, we _will_ have the output file locally.
    # TODO: Why don't we have the output files? Can we ensure output files for CANCEL?
    td.output_staging = []  # TODO(#229) Write and stage output from raptor task.
    td.stage_on_error = True

    td.pre_exec = list(pre_exec)

    # We are not using prepare_env at this point. We use the `venv` configured by the
    # caller.
    # td.named_env = 'scalems_env'

    td.executable = "python3"
    td.arguments = []
    if os.getenv("PYTHONDEVMODE", None) == "1" or "dev" in getattr(sys, "_xoptions", {}):
        td.arguments.extend(("-X", "dev"))
        td.environment["RADICAL_LOG_LVL"] = "DEBUG"
    if os.getenv("COVERAGE_RUN") is not None or os.getenv("SCALEMS_COVERAGE") is not None:
        # TODO: Use the FileStore!
        local_coverage_dir = pathlib.Path("scalems-remote-coverage-dir").resolve()
        td.arguments.extend(
            (
                "-m",
                "coverage",
                "run",
                "--parallel-mode",
                "--source=scalems",
                "--branch",
                "--data-file=coverage_dir/.coverage",
            )
        )
        td.environment["SCALEMS_COVERAGE"] = "TRUE"
        td.environment["RADICAL_LOG_LVL"] = "DEBUG"
        td.pre_exec.append("mkdir -p coverage_dir")
        td.output_staging.extend(
            (
                {
                    "source": "task:///coverage_dir",
                    "target": local_coverage_dir.as_uri(),
                    "action": rp.TRANSFER,
                },
            )
        )
        logger.info(f"Coverage data will be staged to {local_coverage_dir}.")
    else:
        logger.debug("No coverage testing")
    td.arguments.extend(("-m", "scalems.radical.raptor"))

    config_file = await config_future

    # TODO(#75): Automate handling of file staging directives for scalems.file.AbstractFileReference
    # e.g. _add_file_dependency(td, config_file)
    config_file_name = str(td.uid) + "-config.json"
    td.input_staging = [
        {
            "source": config_file.as_uri(),
            "target": f"task:///{config_file_name}",
            "action": rp.TRANSFER,
        }
    ]
    td.arguments.append(config_file_name)

    task_metadata = {"uid": td.uid, "pilot": pilot.uid}

    logger.debug(f"Using {filestore}.")

    await asyncio.create_task(asyncio.to_thread(filestore.add_task, master_identity, **task_metadata), name="add-task")
    # filestore.add_task(master_identity, **task_metadata)

    logger.debug(f"Launching RP raptor scheduling. Submitting {td}.")

    _task = asyncio.create_task(asyncio.to_thread(pilot.submit_tasks, [td]), name="submit-Master")
    await _task
    raptor: rp.raptor_tasks.Raptor = _task.result()[0]

    # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not
    # check type.
    _task = asyncio.create_task(
        asyncio.to_thread(raptor.wait, state=[rp.states.AGENT_EXECUTING] + rp.FINAL),
        name="check-Master-started",
    )
    await _task
    logger.debug(f"Scheduler {raptor.uid} in state {raptor.state}.")
    # TODO: Generalize the exit status checker for the Master task and perform this
    #  this check at the call site.
    if raptor.state in rp.FINAL:
        if raptor.stdout or raptor.stderr:
            logger.error(f"raptor.stdout: {raptor.stdout}")
            logger.error(f"raptor.stderr: {raptor.stderr}")
        logger.debug(str(raptor.as_dict()))
        raise DispatchError(f"Master Task {raptor.uid} unexpectedly reached {raptor.state} during launch.")
    return raptor


def launch_scheduler(
    *,
    pre_exec: typing.Iterable[str],
    pilot: rp.Pilot,
    filestore: FileStore,
    config_file: scalems.file.AbstractFileReference,
) -> rp.raptor_tasks.Raptor:
    """Establish the radical.pilot.raptor.Master task.

    Create a raptor rp.Task (running the scalems_rp_master script) with the
    provided *name* to be referenced as the *scheduler* for raptor tasks.

    Returns the rp.Task for the raptor script once the Master is ready to
    receive submissions.

    Raises:
        DispatchError if the raptor task could not be launched successfully.

    Note:
        Currently there is no completion condition for the raptor script.
        Caller is responsible for canceling the Task returned by this function.


    """
    # define a raptor.scalems raptor and launch it within the pilot
    td = rp.TaskDescription()

    # The raptor uid is used as the `scheduler` value for raptor task routing.
    # TODO(#108): Use caller-provided *name* for master_identity.
    # Master tasks may not appear unique, but must be uniquely identified within the
    # scope of a rp.Session for RP bookkeeping. Since there is no other interesting
    # information at this time, we can generate a random ID and track it in our metadata.
    master_identity = EphemeralIdentifier()
    td.uid = "scalems-rp-raptor." + str(master_identity)

    td.mode = rp.RAPTOR_MASTER

    # scalems_rp_master will write output before it begins handling requests. The
    # script may crash even before it can write anything, but if it does write
    # anything, we _will_ have the output file locally.
    # Note that rp.Task.cancel() sends SIGTERM, which may prevent writing output files.
    # However, as of RP 1.33, the "stop" rpc for Raptor should allow the main script
    # thread to complete normally, as long as the self.join() is reached.
    td.output_staging = []  # TODO(#229) Write and stage output from raptor task.
    td.stage_on_error = True

    td.pre_exec = list(pre_exec)

    # We are not using prepare_env at this point. We use the `venv` configured by the
    # caller.
    # td.named_env = 'scalems_env'

    td.executable = "python3"
    td.arguments = []
    if os.getenv("PYTHONDEVMODE", None) == "1" or "dev" in getattr(sys, "_xoptions", {}):
        td.arguments.extend(("-X", "dev"))
        td.environment["RADICAL_LOG_LVL"] = "DEBUG"
    if os.getenv("COVERAGE_RUN") is not None or os.getenv("SCALEMS_COVERAGE") is not None:
        # TODO: Use the FileStore!
        local_coverage_dir = pathlib.Path("scalems-remote-coverage-dir").resolve()
        td.arguments.extend(
            (
                "-m",
                "coverage",
                "run",
                "--parallel-mode",
                "--source=scalems",
                "--branch",
                "--data-file=coverage_dir/.coverage",
            )
        )
        td.environment["SCALEMS_COVERAGE"] = "TRUE"
        td.environment["RADICAL_LOG_LVL"] = "DEBUG"
        td.pre_exec.append("mkdir -p coverage_dir")
        td.output_staging.extend(
            (
                {
                    "source": "task:///coverage_dir",
                    "target": local_coverage_dir.as_uri(),
                    "action": rp.TRANSFER,
                },
            )
        )
        logger.info(f"Coverage data will be staged to {local_coverage_dir}.")
    else:
        logger.debug("No coverage testing")
    td.arguments.extend(("-m", "scalems.radical.raptor"))

    logger.debug(f"Using {filestore}.")

    # TODO(#75): Automate handling of file staging directives for scalems.file.AbstractFileReference
    # e.g. _add_file_dependency(td, config_file)
    config_file_name = str(td.uid) + "-config.json"
    td.input_staging = [
        {
            "source": config_file.as_uri(),
            "target": f"task:///{config_file_name}",
            "action": rp.TRANSFER,
        }
    ]
    td.arguments.append(config_file_name)

    task_metadata = {"uid": td.uid, "Pilot": pilot.uid}
    filestore.add_task(master_identity, **task_metadata)
    logger.debug(f"Launching RP raptor scheduling. Submitting {td}.")
    return pilot.submit_tasks(descriptions=[td])[0]
