"""Manage the SCALE-MS Workflow Context.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as WorkflowContext instances.

This module allows the Python interpreter to track a global stack or tree
structure to allow for simpler syntax and clean resource de-allocation.
"""

import abc
import contextlib
import contextvars
import functools


# TODO: Incorporate into WorkflowContext interface.
# Design note: WorkflowContext classes could cache implementation details for item types,
# but (since we acknowledge that work may be defined before instantiating the context to
# which it will be dispatched), we need to allow the WorkflowContext implementation to
# negotiate/fetch the implementation details at any time. In general, relationships between specific
# workflow item types and context types should be resolved in terms of context traits,
# not specific class definitions. In practice, we have some more tightly-coupled relationships,
# at least in early versions, as we handle (a) subprocess-type items, (b) function-based items,
# and (c) data staging details for (1) local execution and (2) remote (RADICAL Pilot) execution.
@functools.singledispatch
def workflow_item_director_factory(item, *, context, label: str = None):
    """

    Get a workflow item director for a context and input type.

    When called, the director finalizes the new item and returns a view.
    """
    raise NotImplementedError('No registered implementation for {} in {}'.format(repr(item), repr(context)))


@workflow_item_director_factory.register
def _(item_type: type, *, context, label: str = None):
    # When dispatching on a class instead of an instance, just construct an
    # object of the indicated type and re-dispatch. Note that implementers of
    # item types must register an overload with this singledispatch function.
    # TODO: Consider how best to register operations and manage their state.
    def constructor_proxy_director(*args, **kwargs):
        assert isinstance(item_type, type)
        item = item_type(*args, **kwargs)
        director = workflow_item_director_factory(item, context=context, label=label)
        return director()
    return constructor_proxy_director


class AbstractWorkflowContext(abc.ABC):
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
    def __enter__(self):
        """Initialize context manager and return the entered Session."""
        raise NotImplementedError('Context Manager protocol not implemented for {}.'.format(type(self)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager without processing exceptions."""
        return False
        # TODO: Consider if/how we should process un-awaited tasks.

    # @abc.abstractmethod
    # def add_task(self, operation: str, bound_input):
    #     """Add a task to the workflow.
    #
    #     Arguments:
    #         operation: named operation to perform
    #         bound_input: reference to a workflow object compatible with operation input.
    #
    #     Returns:
    #         Reference to the new task.
    #
    #     TODO: Resolve operation implementation to dispatch task configuration.
    #     """
    #     ...

    @abc.abstractmethod
    def add_task(self, task_description):
        """Add a task to the workflow.

        Returns:
            Reference to the new task.

        TODO: Resolve operation implementation to dispatch task configuration.
        """
        ...


class DefaultContext(AbstractWorkflowContext):
    """Manage workflow data and metadata, but defer execution to sub-contexts.

    Not yet implemented or used.
    """

    def add_task(self, task_description):
        raise NotImplementedError('Trivial work graph holder not yet implemented.')


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