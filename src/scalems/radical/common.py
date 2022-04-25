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
    """
    uid: str
    """Canonical identifier for the Task.

    Note that *uid* may be omitted from the original TaskDescription.
    """

    description: _RaptorTaskDescription
    """Encoding of the original task description."""

    out: str
    """Task standard output."""

    err: str
    """Task standard error."""

    ret: int
    """Function return code."""

    val: typing.Any
    """Function return value."""

    exc: typing.Tuple[str, str]
    """Exception type name and message."""

    state: str
    """RADICAL Pilot Task state."""


class RaptorWorkerConfig(typing.TypedDict):
    """Container for the raptor worker parameters.

    Expanded with the ``**`` operator, serves as the arguments to
    :py:meth:`radical.pilot.raptor.Master.submit_workers`

    The *descr* member represents the *descr* parameter of
    :py:meth:`~radical.pilot.raptor.Master.submit_workers()`,
    pending further documentation.

    Create with `worker_description()`.
    """
    descr: dict
    """Worker description.

    Fields:
        named_env (str): venv for the Worker task.
        worker_class (str): raptor concrete Worker class to base the Worker task on, built-in or custom.
        worker_file (str, optional): module from which to import *worker_class*.

    See Also:
        :py:meth:`~radical.pilot.raptor.Master.submit_workers()`
    """

    count: typing.Optional[int]
    """Number of workers to launch."""


def worker_description(*,
                       uid: str,
                       named_env: str,
                       cpu_processes: int = None,
                       gpu_processes: int = None,
                       ):
    """Get a worker description for Master.submit_workers().

    scalems does not use a custom Worker class for
    :py:meth:`~radical.pilot.raptor.Master.submit_workers()`.
    Instead, a custom dispatching function is injected into the
    Worker environment for dispatching scalems tasks.

    Keyword Args:
        cpu_processes (int, optional): See `radical.pilot.TaskDescription.cpu_processes`
        gpu_processes (int, optional): See `radical.pilot.TaskDescription.gpu_processes`
        named_env (str): Python virtual environment known to the Pilot agent.
            Example: the *env_name* argument to :py:meth:`radical.pilot.Pilot.prepare_env`.
        uid (str): Identifier for the worker task.
    """
    descr = {
        'uid': uid,
        'named_env': named_env,
        'worker_class': 'MPIWorker',
        'worker_file': None,
        'cpu_processes': cpu_processes,
        'gpu_processes': gpu_processes
    }
    return descr
