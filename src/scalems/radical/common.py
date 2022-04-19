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
    'worker_description'
)

import typing
import warnings

try:
    import radical.pilot as rp
    from packaging.version import parse as parse_version
    if parse_version(rp.version) < parse_version('1.14') or not hasattr(rp, 'TASK_FUNCTION'):
        warnings.warn('RADICAL Pilot version 1.14 or newer is required.')
except (ImportError, TypeError):
    warnings.warn('RADICAL Pilot installation not found.')

# We import rp before `logging` to avoid warnings when rp monkey-patches the
# logging module. This `try` suite helps prevent auto-code-formatters from
# rearranging the import order of built-in versus third-party modules.
import logging

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

RaptorReturnType = typing.Tuple[
    typing.Union[None, typing.Text],
    typing.Union[None, typing.Text],
    typing.Union[None, typing.SupportsInt],
    typing.Any
]
"""Raptor worker task return values are interpreted as a tuple (out, err, ret, value).

The first two elements are cast to output and error strings, respectively.

The third is cast to an integer return code.

The fourth is a return value of arbitrary type.

Results of the function call are added as the *val* field of the *result* dictionary member of the
Request object that will be made available to :py:func:`~radical.pilot.raptor.Master.result_cb()`.
**TODO: Is this still correct?**
"""


class _RaptorTaskDescription(typing.Protocol):
    """Describe a Task to be executed through a Raptor Worker.

    A specialization of `radical.pilot.TaskDescription`.

    Note the distinctions of a TaskDescription to processed by a raptor.Master.

    The meaning or utility of some fields is dependent on the values of other fields.
    """
    uid: str
    """Unique identifier for the Task across the Session."""

    executable: typing.ClassVar[str] = 'scalems'
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


_RaptorWorkerTaskDescription = typing.NewType('_RaptorWorkerTaskDescription', dict)
"""Structured data for the raptor worker description.

Represent the *descr* parameter of :py:func:`radical.pilot.raptor.Master.submit_workers`,
pending further documentation.

.. todo:: Worker description input.
    What fields are required in the *descr* Mapping passed to Master.submit_workers()?
    What optional fields are allowed, and what do they mean?

"""


class RaptorWorkerConfig(typing.TypedDict):
    """Container for the raptor worker parameters.

    Expanded with the ``**`` operator, serves as the arguments to
    :py:func:`radical.pilot.raptor.Master.submit_workers`
    """
    descr: _RaptorWorkerTaskDescription
    count: typing.Optional[int]


def worker_description(*,
                       named_env: str,
                       cpu_processes: int,
                       gpu_processes: int,
                       ):
    """Get a worker description for Master.submit_workers().

    scalems does not use a custom Worker class for
    :py:meth:`~radical.pilot.raptor.Master.submit_workers()`.
    Instead, a custom dispatching function is injected into the
    Worker environment for dispatching scalems tasks.

    Keyword Args:
        named_env (str, optional): Python virtual environment known to the Pilot agent.
            Example: the *env_name* argument to :py:meth:`radical.pilot.Pilot.prepare_env`
        cpu_processes (int, optional): See `radical.pilot.TaskDescription.cpu_processes`
        gpu_processes (int, optional): See `radical.pilot.TaskDescription.gpu_processes`
    """
    descr = {
        'named_env': named_env,
        'worker_class': None,
        'worker_file': None,
        'cpu_processes': cpu_processes,
        'gpu_processes': gpu_processes
    }
    return descr
