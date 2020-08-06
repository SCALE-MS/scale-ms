=============
Serialization
=============

Workflow
========

The workflow should generally only need to encode *instance* details
(see :doc:`dataflow`),
but it is convenient to allow ResourceType to be represented in the workflow, as well.
One benefit is that we may then dynamically define resource types,
such as for *ad hoc* scoping or other abstractions.

JSON document schema
--------------------

The work graph has a basic grammar and structure that maps well to common basic data structures,
particularly in Python.
We use JSON for serialization of a Python dictionary.

Integers and floating point numbers are 64-bit.

The JSON data should be utf-8 compatible, but note that JSON codecs probably map Unicode string
objects on the program side to un-annotated strings in the serialized data
(encoding is at the level of the entire byte stream).

Document prototype::

    {
        "version": "scalems_workflow_1",
        "types": {},
        "referents": []
    }

``types`` member keys conform to the :token:`typeIdentifier` grammar.
Member objects are mappings of :token:`label`s to ResourceDescriptions.

A ResourceDescription maps ``type`` and ``shape`` to a
:term:`Resource Type Identifier` array
and a :term:`Resource Shape` array.
ResourceDescription prototype::

    {
        "type": [],
        "shape": []
    }

``referents`` elements represent nodes in a directed acyclic graph (DAG) of
resources and data flow dependencies.
To allow error checking during deserialization,
``referents`` elements must occur in a topologically valid sequence.
Otherwise, the sequence is not specified.
For a sequence to be topologically valid, any references encountered in a
``referents`` element must be resolvable by the WorkFlowContext manager to an
"immutable" resource definition.
(Sequences may be trivially pre-validated in the serialized document by
confirming that ``referents`` elements do not occur after they have been referenced.)
Elements are objects containing at least a ``uid`` and ``type`` key.
Other allowed or required keys are determined by the type specification.
The ``uid`` member conforms to the :token:`uid` grammar.
``type`` holds a :term:`Resource Type Identifier` array.
The ``label`` key is reserved for an (optional) string conforming to the :token:`label` grammar.
Additional members are keyed with :token:`label` grammar.
A member value may be either a :token:`reference` string or an array with type and shape suitable for
the schema.

.. glossary::
    Resource Type Identifier:
        Array of :token:`implementation` element strings, with outer namespace elements occurring first.

    Resource Shape:
        A tuple describing the resource shape, starting with the outer dimensions.

Identifiers
-----------

Names (labels and UIDs) in the work graph are strings from the ASCII / Latin-1 character set.
Periods (``.``) have special meaning as delimiters.

Some restrictions and special meanings are imposed on keys (object names or labels),
given here in `BNF notation <https://www.w3.org/Notation.html>`__.

:token:`objectname`
strings have stricter requirements because they are likely to directly map to
coding constructs, whereas :token:`label` strings are likely to appear only as keys to
associative mappings and may have more relaxed rules. Specifically, :token:`objectname`
must begin with a letter and may not contain hyphens.
Some additional symbols are omitted for conciseness.
These are *string* (a sequence of characters from the *latin-1* character set),
*integer*, and *letter* (the 52 alphabetic characters from *latin-1* in the
contiguous blocks 'a' - 'z' and 'A' - 'Z').

.. seealso:: `Python identifiers <https://docs.python.org/3/reference/lexical_analysis.html#identifiers>`__


.. productionlist:: UserLabel
    objectnamecharacter: underscore | `letter` | `integer`
    labelcharacter: hyphen | underscore | `letter` | `integer`
    objectname: `letter` *objectnamecharacters
    label: labelcharacter *labelcharacter
    subscript: "[" `integer` "]"
    hyphen: "-"
    underscore: "_"


.. productionlist:: reference
    reference: `uid` ["." nestedlabel]
    nestedlabel: `label` [`subscript`] ["." nestedlabel]

.. productionlist:: UID
    uid: 64(DIGIT | [A-F])

.. productionlist:: Type Identifier
    implementation: `objectname`
    implemnetationScope: `objectname` ["." implementationScope]
    typeIdentifier: [implementationScope "."] implementation


Reference semantics
-------------------

JSON does not have a native syntax for internal references, so we define the
following semantics.

A reference (string) may be split at literal "." characters, with the resulting strings
used as the nested keys to locate an object in the current document.
From the first substring that does not exactly match a document key,
processing is deferred to the Workflow Context manager, but subscript syntax
(a trailing substring that begins with "[" and ends with "]")
is assumed to be processed by a slicing protocol.
If the first string is null (reference begins with ".") the reference is
interpreted with semantics determined by the Workflow Context managing the workflow representation.