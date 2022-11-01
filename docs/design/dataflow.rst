=============
Data flow API
=============

Client code describes work by constructing data flow driven graphs of the
operations that implement a research protocol or high level computational
method.
Graphs and adaptive logic are communicated to the execution middleware,
which manages task execution to satisfy the data dependencies for the results
requested by the client script.

Ref: https://github.com/SCALE-MS/scale-ms/wiki/Data-model for the latest details.

Object Model
============

Note that the following discussion does not use "Type" and "Instance" in a way
that necessarily maps to the data model or typing system of any particular programming language.
If it helps you, consider that "XType" and "XInstance" may be facets
of the same software component, or collaborating entities.
An "Object" is intended to refer to a unique entity that participates in the workflow definition.

The implementation of a ScaleMS Command, then, generally implements
facets encompassing the following minimal schema
(some of which may be aliases):

* CommandType
* CommandInputType
* CommandResultType
* CommandInstance
* CommandInputInstance
* CommandResultInstance

A concrete data type could be considered to be a degenerate
version of a Command, in which some facets are self-referential.

At the workflow level, the basic object model looks like this.

.. uml:: diagrams/datamodel_class.puml

This object model is easily represented with schema based on simple structured data,
and therefore easily serialized for portability or check-pointing.

Generally, objects fall in two categories:

* resources
* resource types

A Type (or a reference to a Type) needs to convey enough information
that ScaleMS software can locate implementation details,
including relationships to other Types,
and software modules responsible for providing UI helpers, run time support,
and a method for fingerprinting Instance objects.
A Type is assumed to be immutable and should be defined before it is referenced,
but the definition may be a private detail of a ScaleMS software component.

An Instance has a reference to a Type as part of its prototypical data.
The Instance will contain the same Field names as the corresponding Type,
but with different semantics.
Instance Fields reference Instance objects rather than Types.
Instance *input* Fields are immutable references to Instances providing input.
Instance *results* Fields *may* reference the current instance (such as when
there is no more concrete data source (currently) available),
and may be modified as the workflow state is updated.

This level of representation does not specifically address workflow state,
but some amount of state is appropriate to encode. Namely, we allow that
the result reference of a CommandInstance may point to the current task if the result is not yet known,
or it may reference another object if the command has been deferred/dispatched or resolved to a concrete data instance.

An Instance may be stateful, but must be uniquely identifiable in terms of its immutable details
from the time it is declared.
Its identifier is a ``uid`` that is constructed by "fingerprinting" the node.
A node fingerprint incorporates the semantics of the node
(a unique label of the behavior of the implementing code),
and its inputs (either constant parameters or the ``uid`` of the input source).
The pairing of a ``uid`` with ``results`` references is intended to uniquely
identify results at any point in their lifetime (or before).

.. uml:: diagrams/datamodel_instance.puml