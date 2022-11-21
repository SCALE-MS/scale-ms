"""Support for scalems on radical.pilot.raptor.

Define the connective tissue for SCALE-MS tasks embedded in rp.Task arguments.

Provide the RP Raptor Master and Worker details. Implement the master task
script as a package entry point that we expect to be executable from the
command line in a virtual environment where the scalems package is installed,
without further modification to the PATH.

The client should be reasonably certain that the target environment has a
compatible installation of RP and scalems. A rp.raptor.Master task script is
installed with the scalems package. The script name is provide by the
module function :py:func:`~scalems.radical.raptor.master_script()`, and will
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
     so that a Master can start appropriate Workers.
    end note

    TaskManager -> Queue: task_description
    deactivate TaskManager
    -> TaskManager: submit(task_description)
    activate TaskManager

    'note left
    note over TaskManager
     TaskDescription names a Master uid in *scheduler* field.
    end note

    TaskManager -> Queue: task_description
    activate Queue
    deactivate TaskManager
    == ==
    deactivate Queue

    TaskManager <-- : master task results

A "master task" is an *executable* task (*mode* = ``rp.RAPTOR_MASTER``)
in which the script named by
:py:data:`radical.pilot.TaskDescription.executable`
manages the life cycle of a :py:class:`radical.pilot.raptor.Master`
(or subclass instance).
As of RP 1.14, the protocol is as follows.

.. uml::

    title raptor master task lifetime management

    participant "master task"

    -> "master task" : stage in

    create Master as master
    "master task" -> master: create Master(cfg)
    "master task" -> master: Master.submit_workers(descr=descr, count=n_workers)
    "master task" -> master: Master.start()
    alt optional hook for self-submitting additional tasks
    "master task" -> master: Master.submit(tasks)
    end
    queue scheduler
    scheduler -\\ master : request_cb
    scheduler -\\ master : result_cb
    master -> master: Master.stop() (optional)
    "master task" -> master: Master.join()

    [<-- "master task" : stage out


The *cfg* argument to :py:class:`radical.pilot.raptor.Master` does not currently
appear to be required.

*scalems* encodes worker requirements on the client side. The *scalems* master
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
     class MasterTaskConfiguration

     class ClientWorkerRequirements

     class ScaleMSMaster

     ScaleMSMaster --> MasterTaskConfiguration : launches with

     MasterTaskConfiguration *-- ClientWorkerRequirements

     ClientWorkerRequirements -right-> .RaptorWorkerConfig : worker_description()
    }

    namespace radical.pilot.raptor {
     class Master {
      submit_workers()
     }
     Master::submit_workers .> .RaptorWorkerConfig
    }

    scalems.radical.ScaleMSMaster -up-|> radical.pilot.raptor.Master


.. warning:: The data structure for *descr* may still be evolving.
    See https://github.com/radical-cybertools/radical.pilot/issues/2731

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
    client_runtime -> client_runtime : _get_scheduler()
    activate client_runtime
    client_runtime -> client_runtime : master_script()
    client_runtime -> client_runtime : master_input()
    activate client_runtime
    client_runtime -> client_runtime : worker_requirements()
    activate client_runtime

    note over client_runtime
    TODO: Allocate Worker according to workload
    through a separate call to the running Master.
    end note

    return ClientWorkerRequirements
    return MasterTaskConfiguration

    client_runtime -> client_runtime: launch Master Task
    activate client_runtime

    note over client_runtime, task_manager
    We currently require a pre-existing venv for Master task.
    TODO(#90,#141): Allow separate venv for Worker task and Tasks.
    end note

    client_runtime -> task_manager : submit master task
    task_manager -\\ scheduler

    note over task_manager, scheduler
    Master cfg is staged with TaskDescription.
    end note

    deactivate client_runtime

    note over client_runtime, task_manager
    TODO(#92,#105): Make sure the Worker starts successfully!!!
    end note

    return master task
    return dispatching context


TODO
----
Pass input and output objects more efficiently.

Worker processes come and go,
and are not descended from Master processes (which may not even be on the same node),
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

import argparse
import asyncio
import contextlib
import dataclasses
import functools
import importlib
import importlib.metadata
import json
import os
import sys
import tempfile
import typing
import warnings
from importlib.machinery import ModuleSpec
from importlib.util import find_spec

# TODO(Python 3.9): Remove this conditional when we require Python >= 3.9
if sys.version_info.minor < 9:
    from typing import Generator
else:
    from collections.abc import Generator

import packaging.version

# We import rp before `logging` to avoid warnings when rp monkey-patches the
# logging module. This `try` suite helps prevent auto-code-formatters from
# rearranging the import order of built-in versus third-party modules.
try:
    import radical.pilot as rp
except (ImportError,):
    warnings.warn('RADICAL Pilot installation not found.')

import scalems.radical
from .. import FileReference
from ..context import describe_file
from ..context import FileStore

# QUESTION: How should we approach logging? To what degree can/should we integrate
# with rp logging?
import logging

logger = logging.getLogger(__name__)

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)

api_name = 'scalems_v0'
"""Key for dispatching raptor Requests.

We can use this to identify the schema used for SCALE-MS tasks encoded in
arguments to raptor *call* mode executor functions.
"""

EncodableAsDict = typing.Mapping[str, 'Encodable']
EncodableAsList = typing.List['Encodable']
Encodable = typing.Union[str, int, float, bool, None, EncodableAsDict, EncodableAsList]


# def object_decoder(obj: dict):
#     """Provide the object_hook callback for a JSONDecoder."""


@functools.singledispatch
def object_encoder(obj) -> Encodable:
    """Provide the *default* callback for JSONEncoder."""
    raise TypeError(f'No decoder for {obj.__class__.__qualname__}.')


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


@dataclasses.dataclass
class ClientWorkerRequirements:
    """Client-side details to inform worker provisioning.

    This structure is part of the `scalems.radical.raptor.MasterTaskConfiguration`
    provided to the master script. The master script uses this information
    when calling `worker_description()`.
    """
    named_env: str
    pre_exec: typing.Iterable[str] = ()
    cpu_processes: int = None
    gpus_per_process: int = None


class _MasterTaskConfigurationDict(typing.TypedDict):
    """A `MasterTaskConfiguration` encoded as a `dict`."""
    worker: dict  # dict-encoded ClientWorkerRequirements.
    versioned_modules: typing.List[typing.Tuple[str, str]]


@dataclasses.dataclass
class MasterTaskConfiguration:
    """Input to the script responsible for the RP raptor Master.

    .. todo:: Check the schema for RP.
        See https://github.com/radical-cybertools/radical.pilot/issues/2731

    Produced on the client side with `master_input`. Deserialized in the
    master task (`master`) to get a `RaptorWorkerConfig`.
    """
    worker: ClientWorkerRequirements
    """Client-side details to inform worker provisioning.

    Generated by `worker_requirements()`.
    Used to prepare input for `worker_description()`.
    """

    versioned_modules: typing.List[typing.Tuple[str, str]]
    """List of name, version specifier tuples for required modules."""

    @classmethod
    def from_dict(cls, obj: _MasterTaskConfigurationDict):
        """Decode from a native dict.

        Support deserialization, such as from a JSON file in the master task.
        """
        return cls(worker=ClientWorkerRequirements(**obj['worker']),
                   versioned_modules=list(obj['versioned_modules']))


@object_encoder.register
def _(obj: MasterTaskConfiguration) -> dict:
    return dataclasses.asdict(obj)


@cache
def master_script() -> str:
    """Get the name of the RP raptor master script.

    The script to run a RP Task based on a rp.raptor.Master is installed
    with :py:mod`scalems`. Installation configures an "entry point" script
    named ``scalems_rp_master``, but for generality this function should
    be used to get the entry point name.

    Before returning, this function confirms the availability of the entry point
    script in the current Python environment. A client should arrange for
    the script to be called in the execution environment and to confirm
    that the (potentially remote) entry point matches the expected API.

    Returns:
        str: Installed name of the entry point wrapper for :py:func:`~scalems.radical.raptor.master()`

    """
    try:
        import pkg_resources
    except ImportError:
        pkg_resources = None
    _master_script = 'scalems_rp_master'
    if pkg_resources is not None:
        # It is not hugely important if we cannot perform this test.
        # In reality, this should be performed at the execution site, and we can/should
        # remove the check here once we have effective API compatibility checking.
        # See https://github.com/SCALE-MS/scale-ms/issues/100
        assert pkg_resources.get_entry_info('scalems',
                                            'console_scripts',
                                            'scalems_rp_master').name == _master_script
    return _master_script


async def master_input(*, filestore: FileStore, pre_exec: list, worker_venv: str) -> FileReference:
    """Provide the input file for a SCALE-MS Raptor Master script.

    Args:
        filestore: (local) FileStore that will manage the generated FileReference.

    """
    if not isinstance(filestore, FileStore) or filestore.closed or not filestore.directory.exists():
        raise ValueError(f'{filestore} is not a usable FileStore.')

    # This is the initial Worker submission. The Master may submit other workers later,
    # but we should try to make this one as usable as possible.
    task_metadata = worker_requirements(pre_exec=pre_exec, worker_venv=worker_venv)

    # TODO(#248): Decide on how to identify the workers from the client side.
    # We can't know the worker identity until the master task has called submit_workers()
    # filestore.add_task(worker_identity, **task_metadata)

    # TODO(#141): Add additional dependencies that we can infer from the workflow.
    versioned_modules = (('mpi4py', '3.0.0'), ('scalems', scalems.__version__), ('radical.pilot', rp.version))

    configuration = MasterTaskConfiguration(worker=task_metadata, versioned_modules=list(versioned_modules))

    # Make sure the temporary directory is on the same filesystem as the local workflow.
    tmp_base = filestore.directory
    # TODO(Python 3.10) Use `ignore_cleanup_errors=True`
    tmpdir_manager = tempfile.TemporaryDirectory(dir=tmp_base)
    with tmpdir_manager as tmpdir:
        # Serialize the configuration to a temporary file, then add it to the
        # FileStore to get a fingerprinted, tracked file.
        config_file_name = 'raptor_scheduler_config.json'
        config_file_path = os.path.join(tmpdir, config_file_name)
        with open(config_file_path, 'w') as fh:
            json.dump(configuration, fh, default=object_encoder, indent=2)
        file_description = describe_file(config_file_path, mode='r')
        handle: FileReference = await asyncio.create_task(filestore.add_file(file_description),
                                                          name='add-file')
        try:
            tmpdir_manager.cleanup()
        except OSError:
            logger.exception(f'Errors occurred while cleaning up {tmpdir}.')

    return handle


def worker_requirements(*, pre_exec: list, worker_venv: str) -> ClientWorkerRequirements:
    """Get the requirements for the work load as known to the client.

    TODO: Inspect workflow to optimize reusability of the initial Worker submission.
    """
    # num_workers = 1
    cores_per_worker = 1
    gpus_per_worker = 0

    workload_metadata = ClientWorkerRequirements(named_env=worker_venv,
                                                 pre_exec=pre_exec,
                                                 cpu_processes=cores_per_worker,
                                                 gpus_per_process=gpus_per_worker)

    return workload_metadata


parser = argparse.ArgumentParser(description='Command line entry point for RP Raptor master task.')
parser.add_argument('file', type=str, help='Input file (JSON) for configuring ScaleMSMaster instance.')


def master():
    """Entry point for raptor.Master task.

    This function implements the :command:`scalems_rp_master` entry point script
    called by the RADICAL Pilot executor to provide the raptor master task.

    During installation, the `scalems` build system creates a :file:`scalems_rp_master`
    `entry point <https://setuptools.pypa.io/en/latest/pkg_resources.html#entry-points>`__
    script that provides command line access to this function. The Python signature
    takes no arguments because the function processes command line arguments from `sys.argv`

    .. uml::

        title scalems raptor master task
        !pragma teoz true

        queue "RP runtime" as scheduler

        box execution
        box "scalems.radical" #honeydew
        participant "master task"
        participant ScaleMSMaster as master
        end box
        participant worker.py
        end box

        scheduler -> "master task" : stage in
        ?-> "master task" : scalems_rp_master
        activate "master task"

        note over "master task" : "TODO: Get asyncio event loop?"
        note over "master task" : "TODO: Initialize scalems FileStore"

        "master task" -> "master task" : from_dict
        activate "master task"
        return MasterTaskConfiguration
        "master task" -> master **: create ScaleMSMaster()

        "master task" -> "master task" : with configure_worker()
        activate "master task"
        "master task" -> master : configure_worker()
        activate master
        master -> worker.py ** : with _worker_file()
        activate master
        master -> master : _configure_worker()
        activate master
        master -> master : worker_description()
        activate master
        return
        return WorkerDescription
        deactivate master
        return RaptorWorkerConfig
        'master --> "master task" : RaptorWorkerConfig
        'deactivate master

        "master task" -> master: submit_workers(**config)
        "master task" -> master: start()
        note over "master task" : "TODO: wait for worker"

        ...
        scheduler -\\ master : request_cb
        scheduler -\\ master : result_cb
        ...

        "master task" -> master : ~__exit__
        activate master
        master -> master : ~__exit__
        activate master
        master --> master : stop worker (TODO)
        master -> worker.py !! : delete
        deactivate master
        master -> master: stop() (TBD)
        "master task" -> master: join()
        deactivate master
        deactivate "master task"

        scheduler <-- "master task" : stage out
        deactivate "master task"

    """
    # TODO: Configurable log level?
    logger.setLevel(logging.DEBUG)
    character_stream = logging.StreamHandler()
    character_stream.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    character_stream.setFormatter(formatter)
    logger.addHandler(character_stream)

    if not os.environ['RP_TASK_ID']:
        warnings.warn('Attempting to start a Raptor Master without RP execution environment.')

    args = parser.parse_args()
    if not os.path.exists(args.file):
        raise RuntimeError(f'File not found: {args.file}')
    with open(args.file, 'r') as fh:
        configuration = MasterTaskConfiguration.from_dict(json.load(fh))

    logger.info(f'Launching ScaleMSMaster task with {repr(dataclasses.asdict(configuration))}.')

    _master = ScaleMSMaster(configuration)
    logger.debug(f'Created {repr(_master)}.')

    requirements = configuration.worker

    with _master.configure_worker(requirements=requirements) as worker_submission:
        assert os.path.isabs(worker_submission['descr']['worker_file'])
        assert os.path.exists(worker_submission['descr']['worker_file'])
        logger.debug(f"Submitting {worker_submission['count']} {worker_submission['descr']}")
        _master.submit_workers(**worker_submission)
        message = 'Submitted workers: '
        message += ', '.join(_master.workers.keys())
        logger.info(message)

        _master.start()
        logger.debug('Master started.')

        # Make sure at least one worker comes online.
        # TODO(#253): 'wait' does not work in RP 1.18, but should in a near future release.
        # _master.wait(count=1)
        # logger.debug('Ready to submit raptor tasks.')
        # TODO: Confirm workers start successfully or produce useful error
        #  (then release temporary file).
        # See also https://github.com/radical-cybertools/radical.pilot/issues/2643
        # relevant worker states _master.workers['uid']['state']
        # * 'NEW' -> 'ACTIVE' -> 'DONE'

        _master.join()
        logger.debug('Master joined.')


def _configure_worker(*, requirements: ClientWorkerRequirements, filename: str) -> RaptorWorkerConfig:
    """Prepare the arguments for :py:func:`rp.raptor.Master.submit_workers()`.

    Provide an abstraction for the submit_workers signature and parameter types,
    which may still be evolving.

    See also https://github.com/radical-cybertools/radical.pilot/issues/2731
    """
    assert os.path.exists(filename)
    # TODO(#248): Consider a "debug-mode" option to do a trial import from *filename*
    num_workers = 1
    config: RaptorWorkerConfig = {
        'count': num_workers,
        'descr': worker_description(named_env=requirements.named_env,
                                    worker_file=filename,
                                    pre_exec=requirements.pre_exec,
                                    cpu_processes=requirements.cpu_processes,
                                    gpus_per_process=requirements.gpus_per_process),
    }
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
            if spec and hasattr(spec, 'parent'):
                found_version = importlib.metadata.version(spec.parent)
        except Exception as e:
            logger.debug(f'Exception when trying to find {module}: ', exc_info=e)
    if found_version is not None:
        found_version = packaging.version.Version(found_version)
    return found_version


class ScaleMSMaster(rp.raptor.Master):
    """Manage a RP Raptor scheduler target.

    Extends :py:class:`rp.raptor.Master`.

    We do not submit scalems tasks directly as RP Tasks from the client.
    We encode scalems tasks as instructions to the Master task, which will be
    decoded and translated to RP Tasks by the `ScaleMSMaster.request_cb` and
    self-submitted to the `ScaleMSWorker`.

    Results of such tasks are only available through to the `ScaleMSMaster.result_cb`.
    The Master can translate results of generated Tasks into results for the Task
    carrying the coded instruction, or it can produce data files to stage during or
    after the Master task. Additionally, the Master could respond to other special
    instructions (encoded in later client-originated Tasks) to query or retrieve
    generated task results.
    """

    def __init__(self, configuration: MasterTaskConfiguration):
        """Initialize a SCALE-MS Raptor Master.

        Verify the environment. Perform some scalems-specific set up and
        initialize the base class details.
        """
        # Verify environment.
        for module, version in configuration.versioned_modules:
            logger.debug(f'Looking for {module} version {version}.')
            found_version = _get_module_version(module)
            if found_version is None:
                raise SoftwareCompatibilityError(f'{module} not found.')
            minimum_version = packaging.version.Version(version)
            if found_version >= minimum_version:
                logger.debug(f'Found {module} version {found_version}: Okay.')
            else:
                raise SoftwareCompatibilityError(f'{module} version {found_version} not compatible with '
                                                 f'{version}.')
        # The rp.raptor base class will fail to initialize without the environment
        # expected for a RP task launched by an RP agent. However, we may still
        # want to acquire a partially-initialized instance for testing.
        try:
            super(ScaleMSMaster, self).__init__()
        except KeyError:
            warnings.warn('Master incomplete. Could not initialize raptor.Master base class.')

        # Initialize internal state.
        self._dependents = dict()
        self._submitted = 0
        self._completed = 0
        self._workload = set()
        self.__worker_files = {}

    @contextlib.contextmanager
    def configure_worker(self, requirements: ClientWorkerRequirements) \
            -> Generator[RaptorWorkerConfig, None, None]:
        """Scoped temporary module file for raptor worker.

        Write and return the path to a temporary Python module. The module imports
        :py:class:`scalems.radical.common.ScaleMSWorker` into its module namespace
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
        :py:class:`scalems.radical.common.ScaleMSWorker` into its module namespace
        so that the file and class can be used in the worker description for
        :py:func:`rp.raptor.Master.submit_workers()`
        """
        worker = ScaleMSWorker
        # TODO: Use the Master's FileStore to get an appropriate fast shared filesystem.
        with tempfile.TemporaryDirectory() as tmp_dir:
            filename = self.__worker_files.get(worker, None)
            if filename is None:
                filename = next_numbered_file(dir=tmp_dir, name='scalems_worker', suffix='.py')
                with open(filename, 'w') as fh:
                    fh.writelines([f'from {worker.__module__} import {worker.__name__}\n'])
                self.__worker_files[worker] = filename
            yield filename
        del self.__worker_files[worker]


class WorkerDescriptionDict(typing.TypedDict):
    """Worker description.

    See Also:
        * :py:meth:`~radical.pilot.raptor.Master.submit_workers()`
        * https://github.com/radical-cybertools/radical.pilot/issues/2731

    """
    cores_per_rank: typing.Optional[int]
    environment: typing.Optional[dict]
    gpus_per_rank: typing.Optional[int]
    named_env: typing.Optional[str]
    pre_exec: typing.Optional[list]
    ranks: int
    worker_class: typing.Optional[str]
    worker_file: typing.Optional[str]


class RaptorWorkerConfig(typing.TypedDict):
    """Container for the raptor worker parameters.

    Expanded with the ``**`` operator, serves as the arguments to
    :py:meth:`radical.pilot.raptor.Master.submit_workers`

    The *descr* member represents the *descr* parameter of
    :py:meth:`~radical.pilot.raptor.Master.submit_workers()`,
    pending further documentation.

    Create with `worker_description()`.
    """
    descr: WorkerDescriptionDict
    """Client-specified Worker requirements.

    Used by master script when calling `worker_description()`.

    See Also:
        :py:mod:`scalems.radical.raptor.ClientWorkerRequirements`
    """

    count: typing.Optional[int]
    """Number of workers to launch."""


def worker_description(*,
                       named_env: str,
                       worker_file: str,
                       cores_per_process: int = None,
                       cpu_processes: int = None,
                       gpus_per_process: int = None,
                       pre_exec: typing.Iterable[str] = (), ) -> WorkerDescriptionDict:
    """Get a worker description for Master.submit_workers().

    Keyword Args:
        cores_per_process (int, optional): See `radical.pilot.TaskDescription.cores_per_rank`
        cpu_processes (int, optional): See `radical.pilot.TaskDescription.ranks`
        gpus_per_process (int, optional): See `radical.pilot.TaskDescription.gpus_per_rank`
        named_env (str): Python virtual environment registered with `radical.pilot.Pilot.prepare_env`
            (currently ignored. see #90).
        pre_exec (list[str]): Shell command lines for preparing the worker environment.
        worker_file (str): Standalone module from which to import ScaleMSWorker.

    *worker_file* is generated for the master script by the caller.
    See :py:mod:`scalems.radical.raptor`

    The *uid* for the Worker task is defined by the Master.submit_workers().
    """
    kwargs = dict(cores_per_rank=cores_per_process,
                  environment=None,
                  gpus_per_rank=gpus_per_process,
                  named_env=None,
                  pre_exec=list(pre_exec),
                  ranks=cpu_processes,
                  worker_class='ScaleMSWorker',
                  worker_file=worker_file, )
    # Avoid assumption about how default values in TaskDescription are checked or applied.
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    descr = WorkerDescriptionDict(**kwargs)
    return descr


class ScaleMSWorker(rp.raptor.MPIWorker):
    """Specialize the Raptor MPI Worker for scalems dispatching of serialised work.

    scalems tasks encode the importable function and inputs in the arguments to
    the ... dispatching function, which is available to the `ScaleMSWorker` instance.
    """


def next_numbered_file(*, dir, name, suffix):
    """Get the next available filename with the form {dir}/{name}{i}{suffix}."""
    if not os.path.exists(dir):
        raise ValueError(f'Directory "{dir}" does not exist.')
    filelist = os.listdir(dir)
    template = str(name) + '{i}' + str(suffix)
    i = 1
    filename = template.format(i=i)
    while filename in filelist:
        i += 1
        filename = template.format(i=i)
    return os.path.join(dir, filename)
