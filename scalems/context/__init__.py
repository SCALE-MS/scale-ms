"""Manage the SCALE-MS Workflow Context.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as WorkflowContext instances.

This module allows the Python interpreter to track a global stack or tree
structure to allow for simpler syntax and clean resource de-allocation.
"""


def run(coroutine, **kwargs):
    """Execute the provided coroutine object.

    Abstraction for :py:func:`asyncio.run()`

    Note: If we want to go this route, we should integrate with the
    asyncio event loop policy, or obtain an event loop instance and
    use it w.r.t. run_in_executor and set_task_factory.

    .. todo:: Coordinate with RP plans for event loop contexts and concurrency module executors.

    See also https://docs.python.org/3/library/asyncio-dev.html#debug-mode
    """
    # No automatic dispatching yet. Coroutine must be executable
    # in the current context.
    import asyncio
    from asyncio.coroutines import iscoroutine

    # TODO: Check whether coroutine is already executing and where.
    if iscoroutine(coroutine):
        return asyncio.run(coroutine, **kwargs)

    # TODO: Consider generalized coroutines to be dispatched through
    #     custom event loops or executors.

    raise ValueError('Unrecognized awaitable: {}'.format(coroutine))