External interface requirements
===============================

User interfaces
---------------

Support pure Python user interface.

Work graph state is insulated against corruption by limiting direct user access
to data representing immutable graph resources.

Remote work and data must be robust against interrupted connection to the user.

Software interfaces
-------------------

Where computing environment details cannot be insulated from the user,
they are encapsulated by Context implementations.

External code may be accessed through the Python module import system or through
shell execution.

Communications interfaces
-------------------------

Communications should be abstracted from specific technologies
that may not be available or applicable on all computing resources.

System features
===============

Feature: Manage tasks in support of operations
----------------------------------------------

Operation executable details may be implemented various ways.

.. rubric:: REQ-1: Support pure Python :term:`work`.

.. rubric:: REQ-2: Support :term:`tasks <task>` executed via command line tools.

.. rubric:: REQ-3: Support :term:`tasks <task>` executed via Python bindings to external libraries.

Feature: Environment provisioning
---------------------------------

We must be able to map available computing resources to the capabilities of the
software performing a task.

We may decide not to support all use cases.

* software is single-process and/or single-threaded.
* software assumes ownership of all resources on a node.
* software acceleration may be parameterized at launch to direct
  threads, processes, nodes, or accelerator devices
* software supports negotiation of usable or optimal resources through an API
  or helpers / utilities.

.. rubric:: REQ-4: Support multithreaded :term:`tasks <task>`.

.. rubric:: REQ-5: Support multiprocess :term:`tasks <task>`.

.. rubric:: REQ-6: Support multinode :term:`tasks <task>`.

.. rubric:: REQ-7: Allow :term:`operations <operation>` to employ multi-node MPI.

.. rubric:: REQ-8: Allow operation :term:`instances <operation instance>` to own node resources such as GPUs or pinned processes.

.. rubric:: REQ-9: Allow operation :term:`instances <operation instance>` to own multi-node resources.

.. rubric:: REQ-10: Support :term:`operations <operation>` to negotiate node and multi-node resources.

Feature: State
--------------

A node or graph must always have a well defined :term:`graph state` that is
reconcilable with managed data resources and artifacts.
Outputs, checkpoint data, and :term:`graph state` metadata must be sufficiently
robust and granular to allow effective recovery of interrupted work with
minimal recomputation.

.. rubric:: REQ-11: Work graph state is discoverable and verifiable in terms of data artifacts.

.. rubric:: REQ-12: Completed state.

Executed nodes achieve a final completed state.
Output validity implies and is implied by task completion.
The output of a graph node whose operation previously completed must not
require full reexecution to reestablish graph edges.

.. rubric:: REQ-13: Discoverability of incomplete state.

Artifacts from a task that fails to complete must not affect the final results
of that task (within numeric limits of computational determinism in the code
implementing the operation).
A *completed state* should not experience side effects from an earlier *incomplete state*.

.. rubric:: REQ-14: Recoverability of incomplete state.

Regardless of checkpointing facilities in this API, the API must account for
operations with unintegrated / private checkpoint recovery mechanisms, at the
very least allowing an initialized but incompletely executed node to have a
valid graph state.

.. rubric:: Proposed

Work graph state is the aggregate of individual node states.

.. rubric:: Extension

The API should provide facilities or specifications to support checkpoint
recovery within a single node, and to allow details to be delegated to the
operation implementation.

Feature set: Reusable computing resources
-----------------------------------------

Use cases
~~~~~~~~~

The preference to reuse resources will be affected by

* knowability of upcoming tasks
* variations in the sequence of task resource requirements

.. rubric:: case A

Identical resources are required for a known future task.

.. rubric:: case B

Equivalent resources are required for a known future task.

.. rubric:: case C

Past resources are reusable for a :term:`discovered task`.

Feature set: Scoped data placement
----------------------------------

User stories
~~~~~~~~~~~~

As an :term:`iterative method`,
I want runtime resources to be reusable by sequences of
short tasks so that my work is not bound by the overhead of data transfer and
resource provisioning.

As an :term:`adaptive work flow <adaptive method>`,
I want a way for data to remain "in scope" while
tasks are :term:`discovered <discovered task>` so that data locality optimizations
are possible even when data consumers are not known at launch time.

As an :term:`ensemble simulation method <ensemble method>`,
I want implicit and linked outer dimensions between tasks and data
so that the results of *taskA[i]* executed on *worker[i]* are trivially available
to later *tasks[i]* on *worker[i]*.

Feature: work scoped data
-------------------------

Data scope may span groups of tasks, such as through the declaration of a
subgraph or through other semantics that span multiple operations.

.. rubric:: REQ-15: The scope of results data is not equivalent to the scope of a task.

.. rubric:: REQ-16: The end of data scope may be extended at run time.

E.g. tasks are added.

Feature: Decomposition of outer dimensions
------------------------------------------

.. rubric:: REQ-17: Decomposable outer dimensions for work and data.

Arrays of tasks or data may be represented by the outer dimension of
higher dimensional tasks or data.

The API specifies semantics for identifying decomposable outer dimensions
to allow resource partitioning, or provisioning at reduced scope.

.. rubric:: REQ-18: Deterministic indexing for decomposable outer dimensions.

Indices of outer dimensions are deterministically mappable to lower-dimensional
partitions of work or data, when applicable.

.. rubric:: REQ-19: Reusable decomposition.

Work and data may trivially share decomposition of outer dimensions.
Shared decomposition may be used when provisioning the runtime.

Feature: Dimensionally scoped data
----------------------------------

Results of taskA[i] on worker[i] are available to subsequent tasks on worker[i],
without requiring results of task[i+1] to be available on worker[i].

.. rubric:: REQ-20: Decomposed outer dimensions yield discrete scopes.

.. rubric:: REQ-21: Clear semantics for the scope of a decomposition.

.. rubric:: TBD: Mapping ensembles to workers.

.. rubric:: TBD: Lifetime of parallel data flow or outer-dimension decomposition.

Feature: Opportunistic decomposition heuristics
-----------------------------------------------

May be beyond the scope of the initial implementation.

Example use case: GROMACS simulations impose some constraints on the allocation of
usable parallel resources, but trivially parallel gmxapi 0.0.7 Python level
analysis tasks have the opportunity to use an MPI subcommunicator whose shape
was determined by the simulation work load, without having to perform any
additional negotiation of resource requirements.

Example use case: A lesser degree of high level work load shaping is necessary if tasks
are able to express their hierarchy of decomposable axes of input data so that
the framework can map analysis tasks to the compute resources where data is
already located at levels such as sets of simulations, individual simulations,
or even simulation subdomains.

As a further enhancement, arbitrarily decomposable tasks could be scheduled
according to the expected placement of their consuming tasks. A trivial example
is to represent the placement of an input file as an upstream task.

Nonfunctional requirements
==========================

Performance requirements
------------------------

We have described use cases in which resources may be reused to reduce latency
between tasks.
No quality of service (QoS) specification applies, but efficient execution in
such cases requires latency generally to be < 10 seconds for dependent task
launch after upstream task completion.

We should document the characteristics of cases for which task launch latency
can be categorized within a few orders of magnitude.

Security Requirements
---------------------

Software quality attributes
---------------------------

Other requirements
------------------

Provisions for future development
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The architecture should consider future functionality that is beyond the scope
of the present project.

* New large scale computing environments.
* Provisioning for data transfer and communication modes (both within and between tasks)
  that are either beyond the scope of the present project or inherently external to
  this API.
