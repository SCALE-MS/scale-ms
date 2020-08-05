"""Manage the SCALE-MS Workflow Context.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as WorkflowContext instances.

This module allows the Python interpreter to track a global stack or tree
structure to allow for simpler syntax and clean resource de-allocation.
"""

import concurrent.futures
import contextvars


class AbstractWorkflowContext(concurrent.futures.Executor):
    """Abstract base class for SCALE-MS workflow Contexts.

    A workflow context includes a strategy for dispatching a workflow
    for execution. Instances provide the concurrent.futures.Executor
    interface with support and semantics that depend on the Executor
    implementation and execution environment.

    Notably, we rely on the Python contextmanager protocol to regulate
    the acquisition and release of resources, so SCALE-MS workflow
    contexts do not initialize Executors at creation. Instead,
    client code should use `with` blocks for scoped initialization and
    *shutdown* of Executor roles.

    TODO: Enforce centralization of Context instantiation for the interpreter process.
    For instance:
    * Implement a root context singleton and require acquisition of new Context
      handles through methods in this module.
    * Use abstract base class machinery to register Context implementations.
    * Require Context instances to track their parent Context, or otherwise
      participate in a single tree structure.
    * Prevent instantiation of Command references without a reference to a Context instance.
    """


class DefaultContext(AbstractWorkflowContext):
    """Manage workflow data and metadata, but defer execution to sub-contexts.

    Not yet implemented or used.
    """


# Root workflow context for the interpreter process.
_interpreter_context = DefaultContext()
# Note: asyncio.create_task() automatically duplicates a nested contextvars.Context
# for the new task.
parent = contextvars.ContextVar('parent', default=None)
current = contextvars.ContextVar('current', default=_interpreter_context)


def get_context():
    return current.get()


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