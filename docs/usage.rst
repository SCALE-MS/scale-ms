==============
Usage patterns
==============

Use Case Diagrams and general patterns of interaction with SCALE-MS or
collaborating software.

Application user
================

A human uses an application built on top of the Python data flow client API.

.. uml::

    :Application User: <<Human>>
    :Application User: --> (Interact)
    :Method Application: --|> :Data flow script:
    (Interact) -- :Method Application:
    :Data flow script: -- (Prepare work graph)
    :Data flow script: -- (Dispatch)
    :Data flow script: -- (Retrieve results)
    (Dispatch) -- :Client API implementation:
    (Retrieve results) -- :Client API implementation:
    :Operation package: <<Python module>>
    (Express parameters) -- :Operation package:
    (Interact) ..> (Prepare work graph): <<includes>>
    (Interact) ..> (Retrieve results): <<includes>>
    (Interact) ..> (Dispatch): <<includes>>
    (Prepare work graph) ..> (Express parameters): <<include>>

Script execution
================

A data flow script is executed to produce work that may be dynamically scheduled
and executed in terms of importable Python code.

.. uml::

    :Abstract work graph:
    :Managed data:
    :Client API implementation: <|-- :Task manager:

    :Operation package:

    rectangle "user environment" {
    :Data flow script: -- (Prepare work graph)
    :Data flow script: -- (Dispatch)
    :Data flow script: -- (Retrieve results)
    :Operation package: - (Express parameters)
    (Retrieve results) - :Client API implementation:
    (Prepare work graph) - :Client API implementation:
    (Dispatch) - :Client API implementation:
    (Serialize work) -- :Abstract work graph:
    (Place data) - :Managed data:
    (Prepare work graph) ..> (Express parameters): <<include>>
    (Prepare work graph) ..> (Place data): <<include>>
    (Dispatch) ..> (Serialize work): <<include>>
    (Retrieve results) ..> (Place data): <<include>>
    }

    :Task manager: -- (Generate task)
    :Task manager: - (Evaluate dependencies)
    :Task manager: -- (Evaluate resources)

    rectangle "execution environment" {
    :Task manager: <|-- :Executor:
    :Executor: -- (Execute task)
    (Execute task) - :Operation package:
    :Operation package: -- (Publish data)
    :Abstract work graph: -- (Deserialize work)
    (Place data) as (Place execution data)
    (Publish data) ..> (Place execution data): <<include>>
    (Place execution data) - :Managed data:
    }

    (Generate task) ..> (Deserialize work): <<include>>

Execution of dynamic work
=========================

An abstract work graph guides the generation and placement of tasks and input
data, informed by the availability of operation results that satisfy task data
dependencies or client requests.

.. uml::

    :Abstract work graph: -> (Deserialize work)
    :Task manager: <|-- :Executor:
    (Deserialize work) - :Task manager:
    :Task manager: -- (Generate task)
    :Executor: -- (Execute task)
    :Executor: -- (Generate task)
    :Operation package: - (Execute task)
    :Operation package: - (Publish data)
    (Generate task) ..> (Evaluate resources): <<include>>
    (Generate task) ..> (Evaluate dependencies): <<include>>
    (Generate task) ..> (Control compute and input resources): <<include>>
    (Execute task) ..> (Control compute and input resources): <<include>>
    (Evaluate dependencies) ..> (Place data): <<include>>
    (Publish data) ..> (Place data): <<include>>
    (Publish data) -> (Generate task)
    :Managed data: - (Place data)