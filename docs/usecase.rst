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

Scripting user
==============

Python code written directly against the data flow client library.

.. seealso:: :doc:`dataflow`

Run a sequence of dependent tasks
---------------------------------

| 1. Import data flow client package.
| 2. Add an operation to the work graph.
| 3. Add a dependent operation to the work graph.
| 4. Request results.
|  4.1 Work is implicitly dispatched.
|  4.2 Interpreter blocks until results are available.
|  4.3 API data is localized and converted to native data types.

For a concrete example, consider

Assume *gmxapi* is implemented in terms of the scale-ms data flow client library::

    import gmxapi
    # 1. Default gmxapi Context implemented in terms of SCALE-MS data flow client API.
    # 2. Get a reference to a new graph node.
    some_wrapped_cli_tool = gmxapi.commandline_operation(...)


Ensemble run-analyze-run
------------------------

Iteratively converge
--------------------





Client API library
==================

Use cases encountered in the client library implementation, interacting with
the task execution middleware in support of of client use cases.

.. seealso:: :doc:`dataflow`

Execution middleware
====================

Resource management, data placement, task discovery, and task scheduling
use cases.
Interactions include various architectural aspects,
compute/data/communications abstractions,
quality of service guarantees,
and performance optimizations.

.. seealso:: :doc:`executor`

Generic sequential task execution
---------------------------------

Units of work (nodes) describe the inputs and expected outputs,
and identify a module able to perform the operation.

| 1. Receive a node of work to execute.
| 2. Import a module implementing the operation.
| 3. Initialize or restore checkpoint state.
| 4. Prepare operation resources.
|    3.1 Request or confirm locality of input data.
|    3.2 Create data publishing resource for operation.
|    3.3 Prepare compute environment for execution.
|    3.4 Use operation module helpers to construct input objects.
| 5. Provide resources and execute operation.
| 6. Finalize checkpoint state.
| 7. Note or confirm locality of operation results.
| 8. Get next node of work to execute and proceed return to 2.

Variant: checkpoint state is completed.
| 3a. Checkpoint state appears final. Skip to 7a.
| 7a. Operation helper reports data identifiers for previously published results.



Operation implementation
========================

Use cases encountered when implementing operation code to be executed by the
runtime.

.. seealso:: :doc:`operations`