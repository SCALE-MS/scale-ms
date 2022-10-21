"""Support for scalems on radical.pilot.raptor.

Client:
    The client should be reasonably certain that the target environment has a
    compatible installation of RP and scalems. A rp.raptor.Master task script is
    installed with the scalems package. The script name is provide by the
    module function :py:func:`~scalems.radical.raptor.master_script()`, and will
    be resolvable on the PATH for a Python interpreter process in an
    environment that has the `scalems` package installed.

Define the connective tissue for SCALE-MS tasks embedded in rp.Task arguments.

This module should not depend on any scalems modules, and ultimately may disappear as
the RP.raptor interfaces are formalized.

As of :py:mod:`radical.pilot` version 1.14, the distinct "raptor" Request container
has been merged into an updated TaskDescription. The new TaskDescription supports
a *mode* field and several additional fields. The extended schema for the TaskDescription
depends on the value of *mode*. The structured data provided to the executor callable
is composed from these additional fields according to the particular mode.

We can use the 'call' mode, specifying a *function* field to name a callable in the
global namespace. We can populate the global namespace with imported callables by
subclassing DefaultWorker (or, soon, MPIWorker), or we could define a method on our
subclass, which would then also convey access to any resources available privately
to Worker and its subclasses. However, we expect to have to relaunch the Worker task
if the work load changes substantially, so it should be sufficient to inject imported
callables into the scope of the Worker through the worker script that we prepare or
through

The callable for *call* accepts ``*args`` and ``**kwargs``, which are extracted
from the *data* positional argument (Mapping) of ``Worker._call()``. *data* is
composed from the TaskDescription.

.. todo:: How is *data* composed?

Protocol
--------
A "master task" is an *executable* task in which the script named by
:py:data:`radical.pilot.TaskDescription.executable`
manages the life cycle of a :py:class:`radical.pilot.raptor.Master` (or subclass instance).
As of RP 1.14, the protocol is as follows.

.. uml::

    "master task" -> "master task": create Master(cfg)
    "master task" -> master: Master.submit_workers(descr=descr, count=n_workers)
    "master task" -> master: Master.start()
    alt optional hook for self-submitting additional tasks
    "master task" -> master: Master.submit()
    end
    "master task" -> master: Master.join()
    "master task" -> master: Master.stop()

.. todo:: Master config input.
    What fields are required in the *cfg* input? What optional fields are allowed?
"""
import argparse
import dataclasses
import functools
import importlib
import importlib.metadata
import json
import os
import typing
import warnings
from importlib.machinery import ModuleSpec
from importlib.util import find_spec

import packaging.version

# We import rp before `logging` to avoid warnings when rp monkey-patches the
# logging module. This `try` suite helps prevent auto-code-formatters from
# rearranging the import order of built-in versus third-party modules.
try:
    import radical.pilot as rp
    from packaging.version import parse as parse_version

    if parse_version(rp.version) < parse_version('1.14') or not hasattr(rp, 'TASK_FUNCTION'):
        warnings.warn('RADICAL Pilot version 1.14 or newer is required.')
    from radical.pilot import PythonTask
except (ImportError, TypeError):
    warnings.warn('RADICAL Pilot installation not found.')

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

class _RaptorTaskDescription(typing.TypedDict):
    """Describe a Task to be executed through a Raptor Worker.

    A specialization of `radical.pilot.TaskDescription`.

    Note the distinctions of a TaskDescription to processed by a raptor.Master.

    The meaning or utility of some fields is dependent on the values of other fields.
    """
    uid: str
    """Unique identifier for the Task across the Session."""

    executable: str
    """Unused by Raptor tasks."""

    scheduler: str
    """The UID of the raptor.Master scheduler task."""

    mode: str
    """The executor mode for the Worker to use.

    For ``call`` mode, either *function* or *method* must name a task executor.
    Depending on the Worker (sub)class, resources such as an `mpi4py.MPI.Comm`
    will be provided as the first positional argument to the executor.
    ``*args`` and ``**kwargs`` will be provided to the executor from the corresponding
    fields.
    """

    function: str
    """For 'call' mode, a callable for dispatching.

    The callable can either be a function object present in the namespace of the interpreter launched
    by the Worker for the task, or a `radical.pilot.pytask` pickled function object.
    """

    method: str
    """For 'call' mode, a member function of the Worker for dispatching."""

    args: list
    """For 'call' mode, list of positional arguments to provide to the executor method or function."""

    kwargs: dict
    """For 'call' mode, a dictionary of key word arguments to provide to the executor method or function."""


class TaskDictionary(typing.TypedDict):
    """Task representations seen by *request_cb* and *result_cb*.

    Other fields may be present, but the objects in the sequences provided to
    :py:meth:`scalems.radical.raptor.ScaleMSMaster.request_cb()` and
    :py:meth:`scalems.radical.raptor.ScaleMSMaster.result_cb()` have the following fields.
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
    """Function return code."""

    return_value: typing.Any
    """Function return value."""

    exception: typing.Tuple[str, str]
    """Exception type name and message."""

    state: str
    """RADICAL Pilot Task state."""


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
    """Worker description.

    See Also:
        :py:meth:`~radical.pilot.raptor.Master.submit_workers()`
    """

    count: typing.Optional[int]
    """Number of workers to launch."""


class _ConfigurationDict(typing.TypedDict):
    worker: RaptorWorkerConfig
    versioned_modules: typing.List[typing.Tuple[str, str]]


# @object_encoder.register
# def _(obj: rp.TaskDescription) -> dict:
#     return obj.as_dict()


# TODO: Disambiguate from scalems.radical.runtime.Configuration.
@dataclasses.dataclass
class Configuration:
    """Input to the script responsible for the RP raptor Master.

    .. todo:: Check the schema for RP 1.14
    """
    worker: RaptorWorkerConfig
    versioned_modules: typing.List[typing.Tuple[str, str]]

    @classmethod
    def from_dict(cls: typing.Type['Configuration'],
                  obj: _ConfigurationDict) -> 'Configuration':
        return cls(
            worker=RaptorWorkerConfig(**obj['worker']),
            versioned_modules=list(obj['versioned_modules'])
        )


@object_encoder.register
def _(obj: Configuration) -> dict:
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
        assert pkg_resources.get_entry_info('scalems', 'console_scripts',
                                            'scalems_rp_master').name == _master_script
    return _master_script


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
    def __init__(self, configuration: Configuration):
        for module, version in configuration.versioned_modules:
            logger.debug(f'Looking for {module} version {version}.')
            found_version = _get_module_version(module)
            if found_version is None:
                raise SoftwareCompatibilityError(f'{module} not found.')
            minimum_version = packaging.version.Version(version)
            if found_version >= minimum_version:
                logger.debug(f'Found {module} version {found_version}: Okay.')
            else:
                raise SoftwareCompatibilityError(
                    f'{module} version {found_version} not compatible with {version}.'
                )
        super(ScaleMSMaster, self).__init__()

    def result_cb(self, tasks: typing.Sequence[TaskDictionary]):
        """SCALE-MS specific handling of tasks completed by the collaborating Worker(s).

        Notes:
            The RP task dictionary described is for the MPIWorker in RP 1.14. It will be
            normalized in a future RP release.
        """
        for r in tasks:
            logger.info('result_cb %s: %s [%s]' % (r['uid'], r['state'], str(r['val'])))

    def request_cb(
            self,
            tasks: typing.Sequence[TaskDictionary]
    ) -> typing.Sequence[TaskDictionary]:
        """Allows all incoming requests to be processed by the Master.

        Note that Tasks may not be processed in the same order in which they are submitted
        by the client.

        If overridden, request_cb() must return a :py:class:`list`. The returned list
        is interpreted to be requests that should be processed normally after the callback.
        This allows subclasses of rp.raptor.Master to add or remove requests before they become Tasks.
        The returned list is submitted with self.request() by the base class after the
        callback returns.

        A Master may call *self.request()* to self-submit items (e.g. instead of or in
        addition to manipulating the returned list in *request_cb()*).

        It is the developer's responsibility to choose unique task IDs (uid) when crafting
        items for Master.request().
        """
        return tasks


parser = argparse.ArgumentParser()
parser.add_argument(
    'file',
    type=str,
    help='Input file (JSON) for configuring ScaleMSMaster instance.'
)


def master():
    """Entry point for raptor.Master task.

    This function implements the scalems_rp_master entry point script called by the
    RADICAL Pilot executor to provide the raptor master task.
    """
    if not os.environ['RP_TASK_ID']:
        raise RuntimeError('Raptor Master must be launched by RP executor.')

    args = parser.parse_args()
    if not os.path.exists(args.file):
        raise RuntimeError(f'File not found: {args.file}')
    with open(args.file, 'r') as fh:
        configuration = Configuration.from_dict(json.load(fh))

    # TODO: Configurable log level?
    logger.setLevel(logging.DEBUG)
    character_stream = logging.StreamHandler()
    character_stream.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    character_stream.setFormatter(formatter)
    logger.addHandler(character_stream)

    logger.info(f'Launching ScaleMSMaster task with {repr(dataclasses.asdict(configuration))}.')

    worker_submission = configuration.worker

    _master = ScaleMSMaster(configuration)
    logger.debug(f'Created {repr(_master)}.')

    _master.submit_workers(**worker_submission)
    _workers = list(_master._workers.keys())
    message = 'Submitted workers: '
    message += ', '.join(_workers)
    logger.info(message)

    _master.start()
    logger.debug('Master started.')

    # TODO: Confirm workers start successfully or produce useful error.
    # See also https://github.com/radical-cybertools/radical.pilot/issues/2643

    _master.join()
    logger.debug('Master joined.')
    _master.stop()
    logger.debug('Master stopped.')


def dispatch(*args, comm=None, **kwargs):
    """Unpack and run a task requested through RP Raptor.

    To be implemented in resolution of #108.

    Args:
        *args: list of positional arguments from *args* in the TaskDescription.

    Keyword Args:
        comm (mpi4py.MPI.Comm, optional): MPI communicator to be used for the task.
        **kwargs: dictionary of keyword arguments from *kwargs* in the TaskDescription.

    See Also:
        `scalems.radical.common.scalems_dispatch()`
    """
    ...


def scalems_dispatch(*args, **kwargs):
    """SCALE-MS executor for Worker *call* mode.

    Use this (pickled) function as the *function* value in a
    `radical.pilot.TaskDescription` for raptor-mediated dispatching
    with `scalems`.

    See Also:
        `scalems.radical.raptor.dispatch()`
    """
    return dispatch(*args, **kwargs)


def worker_description(*,
                       named_env: str,
                       pre_exec: typing.Iterable[str] = (),
                       cpu_processes: int = None,
                       gpus_per_process: int = None,
                       ):
    """Get a worker description for Master.submit_workers().

    scalems does not use a custom Worker class for
    :py:meth:`~radical.pilot.raptor.Master.submit_workers()`.
    Instead, a custom dispatching function is injected into the
    Worker environment for dispatching scalems tasks.

    Keyword Args:
        cpu_processes (int, optional): See `radical.pilot.TaskDescription.ranks`
        gpus_per_process (int, optional): See `radical.pilot.TaskDescription.gpus_per_rank`
        named_env (str): Python virtual environment registered with `Pilot.prepare_env`
            (currently ignored. see #90).
        pre_exec (list[str]): Shell command lines for preparing the worker environment.

    The *uid* for the Worker task is defined by the Master.submit_workers().
    """
    descr = WorkerDescriptionDict(
        cores_per_rank=1,
        environment={},
        gpus_per_rank=gpus_per_process,
        named_env=None,
        pre_exec=list(pre_exec),
        ranks=cpu_processes,
        worker_class='MPIWorker',
        worker_file=None,
    )
    return descr
