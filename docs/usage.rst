==============
Usage patterns
==============

Use Case Diagrams and general patterns of interaction with SCALE-MS or
collaborating software.

Software and user roles may look something like this.

.. uml:: diagrams/architectural_roles.puml

The software is packaged something like this.

.. uml:: diagrams/architectural_deployment.puml

Application user
================

A human uses an application built on top of the Python data flow client API.

.. uml:: diagrams/application_user_interaction.puml

Script execution
================

A data flow script is executed to produce work that may be dynamically scheduled
and executed in terms of importable Python code.

.. uml:: diagrams/script_interaction.puml

Execution of dynamic work
=========================

An abstract work graph guides the generation and placement of tasks and input
data, informed by the availability of operation results that satisfy task data
dependencies or client requests.

.. uml:: diagrams/simple_executor_interaction.puml
