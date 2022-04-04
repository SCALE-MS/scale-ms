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

.. todo:: Worker description input.
    What fields are required in the *descr* Mapping passed to Master.submit_workers()? What optional fields are there?
"""

__all__ = (
    'RaptorWorkerConfig',
    'RaptorWorkCallable',
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

RaptorExecutor = typing.Callable[[typing.Mapping], typing.Any]
"""Function or method dispatched for raptor worker 'call' mode.

Constraints on the return type are not strongly specified.
Presumably, they must be :py:mod:`msgpack` serializable.
"""

RaptorWorkData = typing.TypeVar('RaptorWorkData')
"""Argument type for a Raptor task dispatching function.

This is the type of the *data* positional argument constructed for
the callable registered for a raptor task "mode".

Constraints on the data type are not yet well-defined.
Presumably, the object must be "Munch"-able.
"""


class RaptorWorkCallable(typing.Protocol[RaptorWorkData]):
    def __call__(self, data: RaptorWorkData) -> RaptorReturnType:
        ...


class _RaptorTaskDescription(typing.Protocol):
    """Note the distinctions of a TaskDescription to processed by a raptor.Master.

    The single element of *arguments* is a JSON-encoded object that will be
    deserialized (RaptorWorkDescription) as the prototype for the dictionary used to
    instantiate the Request.
    """
    uid: str
    """Unique identifier for the Task across the Session."""

    executable: typing.ClassVar[str] = 'scalems'
    """Unused by Raptor tasks."""

    scheduler: str
    """The UID of the raptor.Master scheduler task."""

    mode: str
    """The executor mode for the Worker to use."""

    function: str
    """For 'call' mode, a callable (discoverable in the Worker task interpreter namespace) for dispatching."""

    method: str
    """For 'call' mode, a member function of the Worker for dispatching."""

    args: list
    """For 'call' mode, list of positional arguments to provide to the executor method or function."""

    kwargs: dict
    """For 'call' mode, a dictionary of key word arguments to provide to the executor method or function."""


# TODO: The worker is now submitted via Master.submit_workers(), and the *descr* argument
#     has an updated schema.
# class _RaptorWorkerTaskDescription(typing.TypedDict):
#     """rp.TaskDescription for Scalems raptor.Worker tasks.
#
#     Note that this is just a rp.TaskDescription.
#     """
#     uid: str  # Unique identifier for the Task across the Session.
#     executable: str  # scalems_rp_worker script.
#     scheduler: str  # The UID of the raptor.Master scheduler task.
#     arguments: typing.Sequence[str]  # Processed by raptor.Master._receive_tasks
#     pre_exec: typing.List[str]
#     # Other rp.TaskDescription fields are available, but unused.
#     # pre_launch: typing.List[str]
#     # pre_rank: typing.Mapping[int, typing.List[str]]
#     # post_exec: typing.List[str]
#     # post_launch: typing.List[str]
#     # post_rank: typing.Mapping[int, typing.List[str]]
#
#
# class RaptorWorkerTaskDescription(_RaptorWorkerTaskDescription, rp.TaskDescription):
#     def __init__(self, *args, from_dict=None, **kwargs):
#         if from_dict is None:
#             from_dict = dict(*args, **kwargs)
#         else:
#             if len(args) or len(kwargs):
#                 raise TypeError('Use only one of the dict signature or the '
#                                 'TaskDescription signature.')
#         rp.TaskDescription.__init__(self, from_dict=from_dict)


_RaptorWorkerTaskDescription = typing.NewType('_RaptorWorkerTaskDescription', dict)


class RaptorWorkerConfig(typing.TypedDict):
    """Signature of the rp.raptor.Master.submit_workers()"""
    descr: _RaptorWorkerTaskDescription
    count: typing.Optional[int]


def worker_description(*, named_env: str, worker_class: str, worker_file: str, cpu_processes: int,
                       gpu_processes: int):
    """Get a worker description for Master.submit_workers().

    Keyword Args
    ------------
    named_env : str
        Python virtual environment known to the Pilot agent.
        Example: the *env_name* argument to :py:meth:`radical.pilot.Pilot.prepare_env`
    worker_class : str
        The string name of a :py:class:`radical.pilot.raptor.Worker` subclass
        to be imported from *worker_file*.
    worker_file : str
        The path (str) to Python module (importable in the target environment) from which
        *worker_class* may be imported to launch a Worker task.
    cpu_processes : int
        See `radical.pilot.TaskDescription.cpu_processes`
    gpu_processes : int
        See `radical.pilot.TaskDescription.gpu_processes`

    TODO
    ----
    Can we skip the *worker_file* and *worker_class* and just inject a dispatching hook?
    """
    descr = {
        'named_env': named_env,
        'worker_class': worker_class,
        'worker_file': worker_file,
        'cpu_processes': cpu_processes,
        'gpu_processes': gpu_processes
    }
    return descr
