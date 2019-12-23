==============
Usage patterns
==============

Use Case Diagrams and general patterns of interaction with SCALE-MS or
collaborating software.

Application user
================

A human uses an application built on top of the Python data flow client API.

Script execution
================

A data flow script is executed to produce work that may be dynamically scheduled
and executed in terms of importable Python code.

Execution of dynamic work
=========================

An abstract work graph guides the generation and placement of tasks and input
data, informed by the availability of operation results that satisfy task data
dependencies or client requests.
