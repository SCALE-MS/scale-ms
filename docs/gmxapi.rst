========================
gmxapi 0.2 specification
========================

Core API concepts
=================

Interfaces and/or Base classes
------------------------------

OperationFactory
~~~~~~~~~~~~~~~~

An OperationFactory receives a Context and Input, and returns an OperationHandle to the caller.

Context
~~~~~~~

Variations include

* GraphContext that just builds a graph that can be serialized for deserialization by another context.
* LaunchContext that processes a graph to be run in appropriate OperationContexts. Produces a Session.
* OperationContext or ImmediateContext that immediately executes the implemented operation

NodeBuilder
~~~~~~~~~~~

``addResource()`` Configures inputs, outputs, framework requirements, and factory functions.
``build()`` returns an OperationHandle

Operation
~~~~~~~~~

The OperationHandle returned by a factory may be a an implementing object or some sort of wrapper or proxy object.
output() provides getters for Results.

Has a Runner behavior.

Result
~~~~~~

gmxapi-typed output data. May be implemented with futures and/or proxies. Provides
extract() method to convert to a valid local owning data handle.

Behaviors
---------

Launcher
~~~~~~~~

Callable that accepts a Session (Context) and produces a Runnable (Operation).
A special case of OperationDirector.

Runner
~~~~~~

Takes input, runs, and returns a Runner that can be called in the same way.

run() -> Runner
run(Runner::Resources) -> Runner

OperationDirector
~~~~~~~~~~~~~~~~~

Takes Context and Input to produce a Runner.

Use a Context to get one or more NodeBuilders to configure the operation in a new context.
Then return a properly configured OperationHandle for the context.

Graph
~~~~~

Can produce NodeBuilders, store directorFactories.

Can serialize / deserialize the workflow representation.

* ``serialize()``
* ``uid()``
* ``newOperation()``: get a NodeBuilder
* ``node(uid)``: getter
* ``get_node_by_label(str)``: find uid
* iterator

OutputProxy
~~~~~~~~~~~

Service requests for Results for an Operator's output nodes.

Input
~~~~~

Input is highly dependent on the implementation of the operation and the context in which
it is executing. The important thing is that it is something that can be interpreted by a DirectorFactory.

Arbitrary lists of arguments and keyword arguments can be accepted by a Python
module director to direct the construction of one or more graph nodes or to
get an immediately executed runner.

GraphNode or serialized Operation Input is accepted by a LaunchContext or
DispatchingContext.

A runner implementing the operation execution accepts Input in the form of
SessionResources.


Operations
----------

Each node in a work graph represents an instance of an Operation.
The API specifies operations that a gmxapi-compliant execution context *should* provide in
the ``gmxapi`` namespace.

All specified ``gmxapi`` operations are provided by the reference implementation in released
versions of the ``gmx`` package. ``gmx.context.Context`` also provides operations in the ``gromacs``
namespace. This support will probably move to a separate module, but the ``gromacs`` namespace
is reserved and should not be reimplemented in external software.

When translating a work graph for execution, the Context calls a factory function for each
operation to get a Director. A Python-based Context *should* consult an internal map for
factory functions for the ``gmxapi`` namespace. **TODO:** *How to handle errors?
We don't really want middleware clients to have to import ``gmx``, but how would a Python
script know what exception to catch? Errors need to be part of an API-specified result type
or protocol, and/or the exceptions need to be defined in the package implementing the context.*


*namespace* is imported (in Python: as a module).

operation is an attribute in namespace that

..  versionadded:: 0.0

    is callable with the signature ``operation(element)`` to get a Director

..  versionchanged:: 0.1

    has a ``_gmxapi_graph_director`` attribute to get a Director

Helper
~~~~~~

Add operation instance to work graph and return a proxy object.
If proxy object has ``input`` or ``output`` attributes, they should forward ``getattr``
calls to the context... *TBD*

The helper makes API calls to the default or provided Context and then asks the Context for
an object to return to the caller. Generally, this is a proxy Operation object, but when the
context is a local context in the process of launching a session, the object can be a
graph Director that can be used to finish configuring and launch the execution graph.

Signatures

``myplugin.myoperation(arg0: WorkElement) -> gmx.Operation``

..  versionchanged:: 0.1

    Operation helpers are no longer required to accept a ``gmx.workflow.WorkElement`` argument.

``myplugin.myoperation(*args, input: inputobject, output: outputobject, **kwargs)``

    inputobject : dict
        Map of named input ports to typed gmxapi data, implicitly mappable Python objects,
        or objects implementing the gmxapi Output interface.

Some operations (``gmx.commandline``) need to provide an ``output`` keyword argument to define
data types and/or placeholders (not represented in the work graph).

    outputobject : dict
        Map of named output ports to

Additional ``args`` and ``kwargs`` may be used by the helper function to set up the work
graph node. Note that the context will not use them when launching the operation, though,
so ....


.. todo::

   Maybe let ``input`` and ``output`` kwargs be interpreted by the helper function, too,
   and let the operation node input be completely specified by ``parameters``?

   ``myplugin.myoperation(arg0: graph_ref, *args, parameters: inputobject, **kwargs)``

.. todo::

   I think we can go ahead and let ``gmx.Operation.input`` and ``gmx.Operation.output``
   implement ``get_item``...

Implementation note: the input and output attributes can have common implementations,
provided with Python "Descriptor"s

Servicing the proxy
~~~~~~~~~~~~~~~~~~~

When the Python client added the operation to the work graph, it used a helper function
to get a reference to an Operation proxy object. This object holds a weak reference to
the context and work graph to which it was added.


Factory
~~~~~~~

get Director for session launch

Director
~~~~~~~~

subscribable to implement data dependencies

``build`` method adds ``launch`` and ``run`` objects to execution graph.

To do: change ``build`` to ``construct``

Session callable
~~~~~~~~~~~~~~~~


``gmxapi`` operations
---------------------

Operation namespace: gmxapi


.. rubric:: operation: make_input

.. versionadded:: gmxapi_graph_0_2

Produced by :py:func:`gmx.make_input`

* ``input`` ports

  - ``params``
  - ``structure``
  - ``topology``
  - ``state``

* ``output`` ports

  - ``params``
  - ``structure``
  - ``topology``
  - ``state``


.. rubric:: operation: md

.. versionadded:: gmxapi_workspec_0_1

.. deprecated:: gmxapi_graph_0_2

Produced by :py:func:`gmx.workflow.from_tpr`

Ports:

* ``params``
* ``depends``


.. rubric:: operation: modify_input

.. versionadded:: gmxapi_graph_0_2

Produced by :py:func:`gmx.modify_input`

* ``input`` ports

  - ``params``
  - ``structure``
  - ``topology``
  - ``state``

* ``output`` ports

  - ``params``
  - ``structure``
  - ``topology``
  - ``state``


``gromacs`` operations
----------------------

Operation namespace: gromacs


.. rubric:: operation: load_tpr

.. versionadded:: gmxapi_workspec_0_1

.. deprecated:: gmxapi_graph_0_2

Produced by :py:func:`gmx.workflow.from_tpr`


.. rubric:: operation: mdrun

.. versionadded:: gmxapi_graph_0_2

Produced by :py:func:`gmx.mdrun`

* ``input`` ports

  - ``params``
  - ``structure``
  - ``topology``
  - ``state``

* ``output`` ports

  - ``trajectory``
  - ``conformation``
  - ``state``

* ``interface`` ports

  - ``potential``


.. rubric:: operation: read_tpr

.. versionadded:: gmxapi_graph_0_2

Produced by :py:func:`gmx.read_tpr`

* ``input`` ports

  - ``params`` takes a list of filenames

* ``output`` ports

  - ``params``
  - ``structure``
  - ``topology``
  - ``state``


Extension API
=============

Extension modules provide a high-level interface to gmxapi operations with functions
that produce Operation objects. Operation objects maintain a weak reference to the
context and work graph to which they have been added so that they can provide a
consistent proxy interface to operation data. Several object properties provide
accessors that are forwarded to the context.

.. These may seem like redundant scoping while operation instances are essentially
   immutable, but with more graph manipulation functionality, we can make future
   operation proxies more mutable. Also, we might add extra utilities or protocols
   at some point, so we include the scoping from the beginning.

``input`` contains the input ports of the operation. Allows a typed graph edge. Can
contain static information or a reference to another gmxapi object in the work graph.

``output`` contains the output ports of the operation. Allows a typed graph edge. Can
contain static information or a reference to another gmxapi object in the work graph.

``interface`` allows operation objects to bind lower-level interfaces at run time.

Connections between ``input`` and ``output`` ports define graph edges that can be
checkpointed by the library with additional metadata.

Python interface
================


:py:func:`gmx.read_tpr` creates a node for a ``gromacs.read_tpr`` operation implemented
with :py:func:`gmx.fileio.read_tpr`

:py:func:`gmx.mdrun` creates a node for a ``gromacs.mdrun`` operation, implemented
with :py:func:`gmx.context._mdrun`

:py:func:`gmx.init_subgraph`

:py:func:`gmx.while_loop` creates a node for a ``gmxapi.while_loop``


Work graph procedural interface
-------------------------------

Python syntax available in the imported ``gmx`` module.

..  py:function:: gmx.commandline_operation(executable, arguments=[], input=[], output=[])

    .. versionadded:: 0.0.8

    lorem ipsum

..  py:function:: gmx.get_context(work=None)
    :noindex:

    .. versionadded:: 0.0.4

    Get a handle to an execution context that can be used to launch a session
    (for the given work graph, if provided).

..  py:function:: gmx.logical_not

    .. versionadded:: 0.1

    Create a work graph operation that negates a boolean input value on its
    output port.

..  py:function:: gmx.make_input()
    :noindex:

    .. versionadded:: 0.1

..  py:function:: gmx.mdrun()

    .. versionadded:: 0.0.8

    Creates a node for a ``gromacs.mdrun`` operation, implemented
    with :py:func:`gmx.context._mdrun`

..  py:function:: gmx.modify_input()

    .. versionadded:: 0.0.8

    Creates a node for a ``gmxapi.modify_input`` operation. Initial implementation
    uses ``gmx.fileio.read_tpr`` and ``gmx.fileio.write_tpr``

..  py:function:: gmx.read_tpr()

    .. versionadded:: 0.0.8

    Creates a node for a ``gromacs.read_tpr`` operation implemented
    with :py:func:`gmx.fileio.read_tpr`

..  py:function:: gmx.gather()

    .. versionadded:: 0.0.8

..  py:function:: gmx.reduce()

    .. versionadded:: 0.1

    Previously only available as an ensemble operation with implicit reducing
    mode of ``mean``.

..  py:function:: gmx.run(work=None, **kwargs)
    :noindex:

    Run the current work graph, or the work provided as an argument.

    .. versionchanged:: 0.0.8

    ``**kwargs`` are passed to the gmxapi execution context. Refer to the
    documentation for the Context for usage. (E.g. see :py:class:`gmx.context.Context`)

..  py:function:: gmx.init_subgraph()

    .. versionadded:: 0.1

    Prepare a subgraph. Alternative name: ``gmx.subgraph``

..  py:function:: gmx.tool

    .. versionadded:: 0.1

    Add a graph operation for one of the built-in tools, such as a GROMACS
    analysis tool that would typically be invoked with a ``gmx toolname <args>``
    command line syntax. Improves interoperability of tools previously accessible
    only through :py:func:`gmx.commandline_operation`

..  py:function:: gmx.while_loop()

    .. versionadded:: 0.1

    Creates a node for a ``gmxapi.while_loop``
