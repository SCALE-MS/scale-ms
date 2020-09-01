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
Member objects are mappings of :token:`label` keys to ResourceDescriptions.

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

.. admonition:: Question

    Should we reserve the JSON "object" format for special value types or allow it for scalems.Mapping values?

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

Raw data
--------

The primary purpose of the workflow record is to convey the workflow topology and other metadata.
Data objects will generally be exchanged through a separate mechanism and bound by :token:`reference` only.
It is convenient to allow trivial and native data to be embedded directly, though,
for readability and as a fallback for workflow context managers for which the workflow
deserialization scheme is an effective mode of data transfer.

All ScaleMS data has shape, so it is convenient to require that (where native raw data is allowed)
JSON arrays indicate a raw data object with mappable type.

Field type dispatching

array: mappable type
string: reference
object: (reserved)
number: (reserved)

Type mapping

integer: scalems.Integer64
float: scalems.Float64
array: nested dimension
object: scalems.Mapping
string: string

To reconcile potential ambiguities between reference strings and string values,
serialized raw data may not include references. This is consistent with the notion
that embedded raw data is static.
Combining data from references and static sources constitutes additional
operations that are reasonable to include in the workflow record.
We may need to better specify data topology operations to allow suppression of
excessive primitive operations in the workflow record.
It may be appropriate to have some operations apply exclusively to references
so that we can easily specify lists and maps in terms of references.

As a workaround, if necessary, we can use
dynamically declared types or subgraphs of primitive operations to encode objects
composed from a combination of static and reference data.

Big question: How do we deal with lists of references?

What do the Iterable interfaces look like?

..  Alternatively, we could allow dynamic declaration of serialization/deserialization
    schemes that include logic for type conversion. For instance, a String is not a
    StringWithRefs, but the raw value `["foo"]`, which maps to a native String, can
    be convertible to the element type of StringWithRefs, such that the raw value of
    `["ref1", ["foo"]]` could be handled by a custom deserializer.

Examples
========


Consider a client issuing the following command::

    scalems.executable('myprogram',
                       arguments=['--num_threads', '8'],
                       input_files={'-i': 'infile'},
                       output_files={'-o': 'outfile'},
                       resources={'ncpus': 8, 'launch_method': 'exec'})

The resulting workflow members are represented similarly in client or execution
contexts.

.. uml:: diagrams/workflow_staging/subprocess_simple_workflow_instance.puml

For completeness, we can show representations of the object-type definitions.

.. uml:: diagrams/workflow_staging/subprocesstypes_instance.puml

Tasks and data types in the "scalems" namespace are built-in and do not
generally need to be explicitly recorded.

The resulting workflow is easily dispatched as a simple work load.

The serialized record of the dispatched subgraph, below, includes type entries
for illustration purposes. The representation is not yet normative, though.
Specification for types is still evolving.

For instance, it is clear that the Field structure ("shape") needs to allow for
certain constraint or placeholder values for array dimension sizes.
We may want to further overload the "shape" elements to constrain the keys or
value descriptions of Mappings.

::

    {
        "version"= "scalems_workflow_1",
        "types"= {
            "scalems.SubprocessInput" = {
                "argv" = { "type"= ["scalems", "String"], "shape"= ["constraints.OneOrMore"] },
                "inputs" = { "type"= ["scalems", "Mapping"], "shape"= [1] },
                "outputs" = { "type"= ["scalems", "Mapping"], "shape"= [1] },
                "stdin" = { "type"= ["scalems", "File"], "shape"= [1] },
                "environment" = { "type"= ["scalems", "Mapping"], "shape"= [1] },
                "resources" = { "type"= ["scalems", "Mapping"], "shape"= [1] }
            },
            "scalems.SubprocessResult" = {
                "exitcode" = { "type"= ["scalems", "Integer"], "shape"= [1] },
                "stdout" = { "type"= ["scalems", "File"], "shape"= [1] },
                "stderr" = { "type"= ["scalems", "File"], "shape"= [1] },
                "file" = { "type"= ["scalems", "Mapping"], "shape"= [1] }
            },
            "scalems.Subprocess" = {
                "input" = { "type"= ["scalems", "SubprocessInput"], "shape"= [1] },
                "result" = { "type"= ["scalems", "SubprocessResult"], "shape"= [1] }
            },
        },
        "referents"= [
            {
                "label"= "input_files",
                "uid"= "aaaa...",
                "type"= ["scalems", "Mapping"],
                "data"= [{"-i"= ["infile"]}]
            },
            {
                "label"= "output_files",
                "uid"= "bbbb...",
                "type"= ["scalems", "Mapping"],
                "data"= [{"-o"= ["outfile"]}]
            },
            {
                "label"= "resource_spec",
                "uid"= "cccc...",
                "type"= ["scalems", "Mapping"],
                "data"= [{"ncpus"= 8, "launch_method"= ["exec"]}]
            },
            {
                "label"= "subprocess_input",
                "uid"= "dddd...",
                "type"= ["scalems", "SubprocessInput"],
                "args"= ["myprogram", "--num_threads", "8"],
                "inputs"= "aaaa...",
                "outputs"= "bbbb...",
                "stdin"= null,
                "environment" = [{}],
                "resources" = "cccc..."
            },
            {
                "label"= "command",
                "uid"= "eeee...",
                "type"= ["scalems", "Subprocess"],
                "input"= "dddd...",
                "result"= "eeee..."
            }

        ]
    }

During execution, the result reference of the command will be updated first to
reference a generated task and later to reference the static result data.
Asynchronously, the submitter of the workflow will receive status updates that
allow it to update its local workflow state as it is able to transfer checkpoint
or result artifacts.
