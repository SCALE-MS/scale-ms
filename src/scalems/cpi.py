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
    "hello",
    "stop",
    "to_raptor_task_metadata",
    "from_raptor_task_metadata",
)

import typing


import scalems.exceptions

CpiOperation = typing.Literal["hello", "stop", "add_item"]


class CpiCall(typing.TypedDict):
    operation: str
    operand: typing.Optional[dict]


class CpiResponse(typing.TypedDict):
    message: typing.Optional[dict]


def hello():
    return CpiCall(operation="hello", operand=None)


def stop():
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
