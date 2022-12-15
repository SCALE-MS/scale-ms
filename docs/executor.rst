==========================
Execution middleware layer
==========================

Executable graphs or graph segments produced by client software are dispatched
and translated for execution on managed computing resources.

Client context
==============

.. automodule:: scalems.execution

See also :ref:`scalems-execution`.

WorkflowContext
===============

Information and resources supporting a defined workflow are managed in a scope
we call a WorkflowContext.
All ScaleMS API calls and references occur in or between the scopes of WorkflowContext instances.
When the ScaleMS API is active, there is a notion of a "current" WorkflowContext.

When the scalems module is simply imported, the default context is able to
discover local workflow state from the working directory and manage a workflow
representation, but it is not able to dispatch the work directly for execution.

Execution dispatching implementations can be chosen from the command line when
the interpreter is launched with Python's ``-m`` command line option.

Alternatively, the script can use a Python Context Manager (a ``with`` block) to
activate a WorkflowContext with an execution dispatcher implementation.

It is inadvisable to activate and execution dispatching environment with
procedural calls because it is more difficult to make sure that acquired resources
are properly released if the interpreter has to terminate prematurely.

Execution dispatching contexts
==============================

Execution dispatching generally uses some sort of concurrency model,
but there is not a consistent concurrency model for all dispatchers.
ScaleMS provides abstractions to insulate scripting from particular implementations
or concurrency primitives (such as the need to call :py:func:`asyncio.run`).

Design notes
------------

Callable and arguments
~~~~~~~~~~~~~~~~~~~~~~

Arguments will be supplied to the callable when the task is launched.
Launching a task requires a callable, arguments for the callable, an active
executor.
For a concurrent.futures.Executor, the callable and arguments are passed to
Executor.submit() to get a Task (Future interface).
For asyncio, it is possible to hold a coroutine object that is not yet executing
(awaitable, but no Future interface), and then pass it to asyncio.create_task to get a Task (Future interface).
Alternatively, callable and args can be passed with an executor to asyncio.Loop.run_in_executor()
to get a asyncio.Future.
In RP, A ComputeUnit can be created without immediate scheduling, then a Task is
acquired by "submit"ing.
The Callable and Arguments distinction, then, is not general. It seems reasonable
to have an encapsulated task factory that can operate in conjunction with an
active execution facility to produce a Future. The details of task description ->
task submission / Future handle -> awaiting on the future can be absorbed into the
NodeBuilder and proxy Future protocols, but we probably need two states in the
command handle to indicate whether a Task has been created yet, and a third state
to indicate its completion (note Future.done() is commonly available.).

Upshot:

1. Handles returned by command helpers may not (yet) be bound to real tasks.
2. Context implementations need to support an add_task protocol that proxies the
   details of converting a task description into a Task instance.
3. A Context<->Context protocol needs to manage Futures that proxy to tasks in
   other Contexts. Only the simple and unified ScaleMS.Future should be exposed
   to ordinary users.
4. We can require Session.run() to be used in the near term to force completion of
   the task protocol, and absorb that behavior into scalems.wait() and scalems.run()
   in the long term.

::

    async def canonical_syntax():
        context = sms_context.get_scope()
        # For testing purposes, we will explicitly create each of the implemented Contexts.
        # Simple use case:
        # Use context manager protocol to initialize and finalize connections and resources.
        with context as session:
            cmd = executable(('/bin/echo', 'hello', 'world'), stdout='filename')
            await session.run(cmd) # Near-term solution to resolve all awaitables explicitly.
            # Alternative not yet implemented:
            #    scalems.wait(cmd.exitcode) # Minimally resolve cmd.exitcode
            assert cmd.exitcode.done()
            # In user-facing Contexts, we can make sure that Future.result() does not require explicit waiting.
            assert cmd.exitcode.result() == 0
            # No `wait cmd` is necessary because the awaitable will be managed by session clean-up, if necessary.
        # Higher-level use cases:
        # * Pass the target context to the command factory to get a command object that is
        #   already bound to a particular execution framework.
        # * Set the Context and then call scalems.run(workflow) to get behavior like
        #   asyncio.run().

Python package details
======================

scalems.messages
----------------

.. automodule:: scalems.messages
    :members:

.. _scalems-execution:

scalems.execution
-----------------

.. py:currentmodule:: scalems.execution

.. autoclass:: AbstractWorkflowUpdater
    :members:

.. autoclass:: RuntimeManager
    :members:

.. autofunction:: manage_execution

scalems.workflow
----------------

.. automodule:: scalems.workflow
    :members:

Backends
--------

See :doc:`invocation` for user facing module interfaces.

Built-in Execution Modules include `scalems.radical` and `scalems.local`

For command line usage, an `backend` should support interaction with the
`scalems.invocation` module.

Support for execution module authors: :py:mod:`scalems.invocation`
==================================================================

.. automodule:: scalems.invocation

A module provides an execution backend with an entry point that calls
`scalems.invocation.run`, providing an appropriate factory function.
For example, in the `scalems.local` module,
:file:`scalems/local/__main__.py` contains::

    if __name__ == '__main__':
        sys.exit(scalems.invocation.run(scalems.local.workflow_manager))

:py:func:`scalems.local.workflow_manager` composes a `scalems.workflow.WorkflowManager`
with an appropriate *executor_factory* and other details for the `scalems.local`
execution backend.

.. autofunction:: scalems.invocation.run

utilities
---------

Execution module authors should also be aware of the following utilities.

.. autofunction:: scalems.utility.parser

.. autofunction:: scalems.utility.make_parser
