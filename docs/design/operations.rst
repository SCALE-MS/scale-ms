==========
Operations
==========

Specify the interface by which functional code units are accessible and executable
by the runtime facilities.

Importable Python modules implement a few basic interfaces
to obtain computational and data resources and publish results,
mediated by the runtime executor.

Framework and object model
==========================

.. rubric:: `Operations <https://github.com/SCALE-MS/scale-ms/issues/14>`__

Explain how operations are structured and implemented.

Runtime executor
================

.. uml:: diagrams/runtime_interface_sequence.puml

Data flow, checkpoint, and resumption
----------------------------------------

The executor *should* make sure the worker is provisioned with operation
dependencies before transmitting a packet of work to be performed,
but the worker still needs to resolve the dependencies for each node.

Note that the abstract work graph contains work that a given worker may not
perform (such as array members), but workers need to be able to identify the
outputs of operations they are not responsible for executing.

Implementation road map
-----------------------

Data staging
~~~~~~~~~~~~

Fundamental data structures, like arrays or array slices, need to be fingerprinted
and staged to the worker, and reference-able in concrete graph records.

Concrete task
~~~~~~~~~~~~~

First, consider a graph with no abstraction. Each operation depends on concrete
data explicitly represented in the work record.

.. rubric:: `scale-ms-15 <https://github.com/SCALE-MS/scale-ms/issues/15>`__

* Generate fingerprint for data and operations.
* Produce serialized data node.
* Produce serialized operation node.
* Identify data by fingerprint and localize/confirm availability.
* Deserialize work, (re)initializing local resources. Determine completion status.
* Identify data flow constraints, establish subscriptions and/or chase references.
* Execute and/or publish results to subscribers.

Generating concrete tasks
~~~~~~~~~~~~~~~~~~~~~~~~~

In the simplest abstraction, an operation input is expressed in terms of another
operation. Demonstrate how this is translated to a concrete executable task,
with implications for checkpointing and resumption.

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

Simple work
===========

A single node of work may be executed immediately in the Worker Context,
or not at all (if the local checkpoint state is already final).

.. uml:: diagrams/runtime_immediate_sequence.puml

Deferred execution
==================

Multiple graph nodes may be received in the same packet of work, or asynchronously.
The executor may locally manage dependencies to optimize execution and data placement.

The following diagram is somewhat speculative.
See also :issue:`15` and :issue:`23`.

.. uml:: diagrams/runtime_deferred_sequence.puml


Orphaned diagrams
=================

TODO: Are these redundant? Are they useful?

.. uml:: diagrams/client_context_sequence.puml

.. uml:: diagrams/runtime_checkpoint_usecase.puml
