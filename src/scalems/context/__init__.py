"""Workflow scope and execution environment context management.

SCALE-MS optimizes data flow and data locality in part by attributing all
workflow references to well-defined scopes. Stateful API facilities, workflow
state, and scoped references are managed as WorkflowManager instances.

This module supports scoped_context() and get_context() with internal module state.
These tools interact with the context management of the asynchronous dispatching,
but note that they are not thread-safe. scoped_context() should not be used in
a coroutine except in the root coroutine of a Task or otherwise within the scope
of a contextvars.copy_context().run(). scalems will try to flag misuse by raising
a ProtocolError, but please be sensible.
"""

__all__ = []

import contextlib
import contextvars
import logging

from scalems.exceptions import ProtocolError

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

# Identify an asynchronous Context. Non-asyncio-aware functions may need to behave
# differently when we know that asynchronous context switching could happen.
# We allow multiple dispatchers to be active, but each dispatcher must
# 1. contextvars.copy_context()
# 2. set itself as the dispatcher in the new Context.
# 3. run within the new Context.
# 4. ensure the Context is destroyed (remove circular references)
_dispatcher = contextvars.ContextVar('_dispatcher')

# If we require an event loop to be provided to the WorkflowManager, then
# we should not instantiate a default context on module import. We don't really
# want to hold an event loop object at module scope, and we want to give as much
# opportunity as possible for the caller to provide an event loop.
# # Root workflow context for the interpreter process.
# _interpreter_context = DefaultContext()

# Note: Scope indicates the hierarchy of "active" WorkflowManager instances
# (related by dispatching).
# This is separate from WorkflowManager lifetime and ownership.
# WorkflowManagers should track their own activation status and provide logic for
# whether to allow reentrant dispatching.
# TODO: Shouldn't the previous "current" be notified or negotiated with? Should we be
#  locking something?
# Note that it makes no sense to start a dispatching session without concurrency,
# so we can think in terms of a parent context doing contextvars.copy_context().run(...)
# I think we have to make sure not to nest scopes without a combination of copy_context
# and context managers, so we don't need to track the parent scope. We should also be
# able to use weakrefs.
current_scope = contextvars.ContextVar('current_scope')


def get_context():
    """Get a reference to the manager of the current workflow scope."""
    # TODO: Redocument and adjust semantics.
    # The contextvars and get_context should only be used in conjunction with
    # a workflow_scope() context manager that is explicitly not thread-safe, but
    # which can employ some checks for non-multi-threading access assumptions.
    # get_context() is used to determine the default workflow manager when *context*
    # is not provided to scalems object factories, scalems.run(), scalems.wait() and
    # (non-async) `result()` methods. Default *context* values are a user convenience
    # and so should only occur in the root thread for the UI / high-level scripting
    # interface.
    # Async coroutines can safely use get_context(), but should not use the
    # non-async workflow_scope() context manager for nested scopes without wrapping
    # in a contextvars.run().
    from scalems.workflow import Scope

    try:
        _scope: Scope = current_scope.get()
        current_context = _scope.current
        logger.debug(f'Scope queried with get_context() {repr(current_context)}')
        # This check is in case we use weakref.ref:
        if current_context is None:
            raise ProtocolError('Context for current scope seems to have disappeared.')
    except LookupError:
        logger.debug('Scope was queried, but has not yet been set.')
        current_context = None
    return current_context


@contextlib.contextmanager
def scope(context):
    """Set the current workflow management within a clear scope.

    Restore the previous workflow management scope on exiting the context manager.

    Within the context managed by *scope*, get_context() will return *context*.

    Not thread-safe. In general, this context manage should only be used in the
    root thread.
    """
    from scalems.workflow import Scope

    parent = get_context()
    dispatcher = _dispatcher.get(None)
    if dispatcher is not None and parent is not dispatcher:
        raise ProtocolError(
            'It is unsafe to use concurrent scope() context managers in an asynchronous '
            'context.')
    logger.debug('Entering scope of {}'.format(str(context)))
    current = context
    token = current_scope.set(
        Scope(
            parent=parent,
            current=current)
    )
    if token.var.get().parent is current:
        logger.warning('Unexpected re-entrance. Workflow is already managed by '
                       f'{repr(current)}')
    if token.old_value is not token.MISSING and token.old_value.current != \
            token.var.get().parent:
        raise ProtocolError(
            'Unrecoverable race condition: multiple threads are updating global context '
            'unsafely.')
    # Try to confirm that current_scope is not already subject to modification by another
    #  context manager in a shared asynchronous context.
    # This nesting has to have LIFO semantics both in and out of coroutines,
    # and cannot block.
    # One option would be to refuse to nest if the current scope is not the root scope and
    # the root scope has an active dispatcher. Note that a dispatcher should use
    # contextvars.copy_context().run() and set a new root context.
    # Alternatively, we could try to make sure that no asynchronous yields are allowed
    # when the current context is a nested scope within a dispatcher context,
    # but technically this is okay as long as a second scope is not nested within the
    # first from within a coroutine that might not finish until after the first scope
    # finishes.
    try:
        yield current
    finally:
        """Exit context manager without processing exceptions."""
        logger.debug('Leaving scope of {}'.format(str(context)))
        # Restore context module state since we are not using contextvars.Context.run()
        # or equivalent.
        if token.var.get().parent is not parent or token.var.get().current is not current:
            raise ProtocolError(
                'Unexpected re-entrance. Workflow scope changed while in context '
                f'manager {repr(current)}.')
        else:
            token.var.reset(token)
