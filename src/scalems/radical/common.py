"""Common interfaces for scalems and radical.pilot.

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

__all__ = (
    'RaptorWorkerConfig',
    'TaskDictionary',
    'worker_description'
)

import typing
import warnings

try:
    import radical.pilot as rp
    from packaging.version import parse as parse_version

    if parse_version(rp.version) < parse_version('1.14') or not hasattr(rp, 'TASK_FUNCTION'):
        warnings.warn('RADICAL Pilot version 1.14 or newer is required.')
    from radical.pilot import PythonTask

    pytask = PythonTask.pythontask
except (ImportError, TypeError):
    warnings.warn('RADICAL Pilot installation not found.')

    def pytask(func):
        return func

# We import rp before `logging` to avoid warnings when rp monkey-patches the
# logging module. This `try` suite helps prevent auto-code-formatters from
# rearranging the import order of built-in versus third-party modules.
import logging

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


@pytask
def scalems_dispatch(*args, **kwargs):
    """SCALE-MS executor for Worker *call* mode.

    Use this (pickled) function as the *function* value in a
    `radical.pilot.TaskDescription` for raptor-mediated dispatching
    with `scalems`.

    See Also:
        `scalems.radical.raptor.dispatch()`
    """
    from scalems.radical.raptor import dispatch
    return dispatch(*args, **kwargs)


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
