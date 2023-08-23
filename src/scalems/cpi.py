"""Data models for calls and responses in the Compute Provider Interface.

The data model is immature and evolving. Use the utility functions to create,
encode/decode, and interpret objects.
"""

from __future__ import annotations

__all__ = (
    "CpiCall",
    "CpiResponse",
    "CpiOperation",
    "add_item",
    "exit_scope",
    "hello",
    "start_scope",
    "stop",
    "to_raptor_task_metadata",
    "from_raptor_task_metadata",
)

import typing


import scalems.exceptions

CpiOperation = typing.Literal["add_item", "exit_scope", "hello", "start_scope", "stop"]


class CpiCall(typing.TypedDict):
    operation: str
    operand: typing.Optional[dict]


class CpiResponse(typing.TypedDict):
    message: typing.Optional[dict]


def hello():
    """Elicit a response from the servicer.

    The response type is not (yet) specified, but a non-null non-False response
    indicates that the servicer is known to be ready to accept work.
    """
    return CpiCall(operation="hello", operand=None)


def stop():
    """Direct the servicer to shut down cleanly.

    The client declares it will not ask for additional resources.

    After STOP, no new work will be accepted from the client, though channel may remain open
    until a firmer END is issued or until disconnected at either end.
    Idempotent."""
    return CpiCall(operation="stop", operand=None)


def add_item(item):
    # In prototyping, we used a TypedDict (scalems.radical.raptor.ScalemsRaptorWorkItem)
    # but we aren't really using this operation yet, and we should promote the specification
    # of the operand type to a scope such as this module before expanding its use.
    #
    # Among the shortcomings of the current scheme are insufficient data for describing a
    # RP Task. At the very least, we need to include the number of processes, number of cores
    # per process, GPU requirements, process type (rp.SERIAL vs. rp.MPI), and threading type.
    #
    # For now:
    if not all(key in item for key in ("func", "module", "args", "kwargs", "comm_arg_name")):
        raise ValueError("Unexpected operand for 'add_item'.")
    return CpiCall(operation="add_item", operand=item)


class WorkerRequirements(typing.TypedDict):
    """Resource and environment requirements for CPI Task Workers."""

    cores_per_process: int
    """Number of CPU cores to allocate for each process."""

    processes: int
    """Number of processes per worker."""

    process_type: str
    """Select a process launcher or other optional infrastructure.

    E.g. "mpi" will cause the default MPI process launcher to be used, even if
    *processes*=1.
    """

    thread_type: str
    """Threading model (if any) to configure.

    For example, "openmp" would cause OMP_NUM_THREADS to be set equal to *cores_per_process*.
    """

    gpus: int
    """Number of GPUs to allocate for the Worker.

    Note that the execution backend (RADICAL Pilot) may have limitations on
    how GPU devices are mappable to CPU processes. The number of GPUs requested
    must be commensurate with other parameters and with the constraints of the backend.
    """

    gpu_type: str
    """Additional constraints on the GPU resource request or process environment preparation."""

    initialization: list[str]
    """Shell expressions to evaluate before launching the Worker."""


def worker_requirements(
    *,
    cores_per_process: int = 1,
    gpus: int = 0,
    gpu_type: typing.Optional[str] = None,
    initialization: list[str] = (),
    processes: int = 1,
    process_type: typing.Optional[str] = None,
    thread_type: typing.Optional[str] = None,
) -> WorkerRequirements:
    """Apply appropriate default logic for normalized WorkerRequirements."""
    if gpu_type is None:
        gpu_type = ""
    initialization = list(initialization)
    if process_type is None:
        process_type = ""
    if thread_type is None:
        thread_type = ""
    reqs = dict(
        cores_per_process=cores_per_process,
        processes=processes,
        process_type=process_type,
        thread_type=thread_type,
        gpus=gpus,
        gpu_type=gpu_type,
        initialization=initialization,
    )
    return reqs


class ScopeRequirements(typing.TypedDict):
    """The operand schema for a START_SCOPE call."""

    workers: list[WorkerRequirements]


class ScopeDescription(typing.TypedDict):
    """Response from a StartScope command."""

    raptor_id: str
    scope_id: str
    worker_uids: list[str]


def start_scope(requirements: ScopeRequirements):
    """Enter a CPI scope for the indicated work load requirements.

    Direct an update to the workflow record, such as to describe a data source,
    register a resource, or add a task.
    Specify the resource requirements for task Executor(s). Get an identifier
    to use when submitting tasks under the allocated resources.

    Follow with a ExitScope command (before any new StartScope commands) to
    ensure proper handling of execution dependencies and to avoid unexpected
    resource contention.

    Prepare the operand with the :py:func:`worker_requirements` utility function.

    The expected response is a JSON-serialized dict representation of a `ScopeDescription`.

    Design note:
        WorkflowManager.add_item() and CommandQueueAddItem are not normalized or
        normative. We can start engineering scalems.messages.AddItem
        and scalems.radical.raptor.CpiAddItem together from the bottom up and then
        make another pass at the higher level / entry point interface.

    """
    return CpiCall(operation="start_scope", operand=requirements)


def exit_scope(scope_id: str):
    """Declare the end of a CPI session scope.

    Following a START_SCOPE command, declare that no further tasks will be submitted
    by the client under the named Scope. Allow the Executor to release resources
    as remaining tasks complete.

    The message payload is a string identifying the resource allocation scope to exit.

    Resources will be deallocated when the executor associated with the scope has
    no more queued work, or as soon as possible after a STOP is received.
    """
    return CpiCall(operation="exit_scope", operand={"scope_id": scope_id})


def to_raptor_task_metadata(obj: CpiCall) -> tuple[str, None | dict]:
    """Utility for interfacing with the rp.Task-encoded Raptor messages.

    Design note:
        We want and expect the `CpiCall` instances to be simple and easily serialzed,
        but we don't want to assume that the instances will always be trivially
        serializable and deserializable by the machinery around the
        :py:attr:`rp.TaskDescription.metadata` field. This function provides a
        hook (symmetric with `from_raptor_task_metadata`) to maintain single-call
        transcoding through future evolution of `CpiCall` or RP.
    """
    operation = obj["operation"]
    operand = None
    if operation == "add_item":
        operand = obj["operand"]
        assert isinstance(operand, dict)
    return obj["operation"], operand


def from_raptor_task_metadata(obj: typing.Sequence[str, None | dict]) -> CpiCall:
    if len(obj) != 2:
        raise scalems.exceptions.APIError(
            "rp.Task metadata encoding is a single key-value pair (Sequence[str, None|dict]) for operation and operand."
        )
    operation, operand = obj
    if operation == "add_item":
        return add_item(operand)
    if operation == "stop":
        return stop()
    if operation == "hello":
        return hello()
