==========
Operations
==========

Specify the interface by which functional code units are accessible and executable
by the runtime facilities.

Describe the interface helpers to establish API-compatibility and to support
high level abstract operation handles.

Importable Python modules implement a few basic interfaces
to obtain computational and data resources and publish results,
mediated by the runtime executor.

Overview
========

SCALE-MS compatible operations have a consistent user interface by which
handles to operation nodes are obtained. The handle is effectively a view into
a work graph managed by the framework, and can be used to acquire Futures for
operation results.

.. uml::

    actor script
    participant "Python interpreter" as py

    box "Operation package"
    participant "factory" as factory
    end box

    box "SCALE_MS package"
    participant Context as scalems
    end box


    script -> py: my_op = hello.hello_world()
    py -> factory
    factory -> scalems
    scalems -> handle
    factory --> py: handle

    script -> py: my_result = my_op.output.result()
    py -> handle: output
    handle -> scalems: node output proxy
    py -> scalems: output.result()

        rnote right of scalems #FFFFFF
        {
        "operation": ["hello", "hello_world"],
        "input": {},
        "output": {
            "meta": {
                "resource": {
                    "type": "scalems.String",
                    "shape": [1]
                    },
                },
            },
        }
        endrnote
    scalems -> scalems: dispatch work
    scalems --> py: reference to Python string "hello world"

Whether operation packages directly depend on a scale-ms package or merely
use an interface standard has minimal technical consequences.

.. uml:: diagrams/simple_operation_client_sequence.puml

The framework is only required to instantiate operation resources and function
objects when data flow dependencies need to be resolved, at which point abstract
representations of work are converted to concrete tasks.

.. uml:: diagrams/simple_operation_execution_sequence.puml

The diagrams so far have been simplistic. When we consider the sequence diagrams
in more detail, we actually demonstrate that the protocols and object models
can be very unified for both abstract and concrete work. We use a Context
abstraction to encapsulate the management of a work graph, and use Context
instances to aid in dispatching the participants in Director/Builder interactions.

Operation implementations, then, only need to collaborate with a Context interface,
but must participate in

* self-describing their inputs and outputs
* providing Builders for function objects (or adapters) with standard interfaces
* uniquely characterizing opaque results, or providing translation to express
  abstractly typed results in terms of types that are either fundamental to the
  API, or described by other dispatchable code.

Framework and object model
==========================

.. rubric:: `Operations <https://github.com/SCALE-MS/scale-ms/issues/14>`__

.. Explain how operations are structured and implemented.

.. uml:: diagrams/operation_factory.puml

The basic protocol for obtaining a handle to an operation node allows
dispatching logic to dispatch or compose a Director to translate client input
and operation details to a context-dependent node Builder. The protocol does
not imply whether the operation is executed at this time (or any other), and
the object returned by the builder may be a light-weight token for work
managed by the context, a handle to a function object that has already executed,
or something more exotic, like an adapter that maps a Future interface to a
Builder for a callable task.

Compare the details of an operation handle generated in a client context for
deferred-execution work with the run time concrete node.

.. uml:: diagrams/operation_graph_sequence.puml

.. uml:: diagrams/operation_launch_sequence.puml

Runtime
=======

.. How are operations actually executed?

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