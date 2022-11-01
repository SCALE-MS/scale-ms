=========
Use Cases
=========

Identify and categorize use cases and scenarios motivating SCALE-MS design and
packaging.

Application user
================

High level use cases for applications built on the Python client library.

Application code
================

Use cases encountered when implementing user interfaces or high-level applications
with the Python data flow client library.

Adaptive MSM
------------
| level: algorithm
| Synchronous fixed-width ensemble scenario:
| 1: Set up an array of N simulations, starting from a single input.
| 2: Evaluate "while loop" with ensemble condition.
| 2a: Publish MSM data if converged.
| 2b: Execute an iteration before reevaluating loop condition.
|     .1: For each ensemble member, prepare simulation input for next iteration.
|     .2: Perform N uncoupled MD simulation segments.
|     .3: :ref:`Collect conformation samples across ensemble use case`.
|     .4: Apply new samples to update MSM state.
|         .1: Update transition matrix.
|         .2: Calculate convergence metric.
|         .3: Generate N conformations for the next iteration.

.. _Collect conformation samples across ensemble use case:

Collect conformation samples across ensemble
--------------------------------------------
level: scripting construct / algorithm building block

Use case description:
If N trajectory segments contain M conformation samples, each,
create a reference to the collection of X=N*M samples.

Scenarios: Data transformation and placement scenarios depend on how the reference is
consumed.

Variant: For some data flow topologies, this equates to an AllGather step before
downstream operations.

Variant: In the absence of any other facilities, this is typically accomplished
for GROMACS data by performing :command:`trajcat` on the ensemble trajectory files.

Scripting user
==============

Python code written directly against the data flow client library.

Client API library
==================

Use cases encountered in the client library implementation, interacting with
the task execution middleware in support of of client use cases.

Execution middleware
====================

Resource management, data placement, task discovery, and task scheduling
use cases.
Interactions include various architectural aspects,
compute/data/communications abstractions,
quality of service guarantees,
and performance optimizations.

Generic sequential task execution
---------------------------------

Units of work (nodes) describe the inputs and expected outputs,
and identify a module able to perform the operation.

Operation implementation
========================

Use cases encountered when implementing operation code to be executed by the
runtime.
