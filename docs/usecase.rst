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
