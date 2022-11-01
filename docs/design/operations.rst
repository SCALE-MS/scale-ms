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
