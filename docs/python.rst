========================
Python package reference
========================

.. automodule:: scalems

Object model
============

When the *scalems* package is imported, a default Context is instantiated to
manage the API session. The Python scripting interface allows a directed acyclic
graph of Resources and resource dependencies to be declared for management by
the context. Resources may be static or stateful. Resources have *type* and *shape*.
Resource type may be a fundamental data type or a nested and structured type.
An Operation is a stateful Resource whose type maps to a scalems compatible Function.

Interfaces
==========

scalems Resource references are proxies to resources managed by the framework.

.. todo:: Allow Resource subscription.

A Resource reference may be used as input to a scalems compatible function.

A Resource provides a Future interface if the Resource represents an immutable
data event that can be converted to concrete data in the client context.
Future.result() forces the framework to resolve any pending dependencies and
blocks until a local object can be provided to the caller.

.. todo:: Clarify implicit conversion.

    E.g. For an integer result of shape (1,), do we return a PyInt, an integer
    buffer of shape (1,), or some view reference with an __int__ method? Does it
    also provide a __float__ method?

.. todo:: Explicit slicing protocol.

    I suggest we disambiguate what ``result()`` should and should not do by
    implementing ``result[]`` (``__getitem__``,
    and (where applicable) ``__contains__``, ``__iter__``, and ``__len__``).
    The return value of the indexed or sliced access should be modeled on numpy
    array access semantics, or simply implemented by obtaining a memoryview and
    proxying the ``__getitem__()`` argument through the numpy API.

When a client uses a function to add work to a work flow,
the function returns a reference to an Operation.

.. todo:: Compose the output data proxy directly into the Operation interface.

    Eliminate the nested ``output`` layer from Operation accessors. Use descriptor
    typing and helpers like a ``.outputs()`` member if we need disambiguation.

.. todo:: Allow top-level Resource interface on Operation references.

    An Operation may appear as a single resource or a collection of resources.

An Operation reference has (read-only) attributes for the named resources it
provides. These resources may be nested.

Operations provide a ``run()`` method to force execution at the point of call.
``run()`` is an alias for ``Resource.result()``

.. topic:: Generated Resources.

    Operations depend on input Resources provided to the Function. If native Python
    data is provided to the Function as input, the Function also generates static
    Resources to be placed on the graph. If the Operation is being created in a
    different context than that of a resource dependency, the contexts are
    responsible for fulfilling the dependency.
    The mechanism (subscription, transfer of ownership, etc.) is a detail of the
    Context collaboration.

Execution Module
================

Every SCALE-MS object reference belongs to a workflow managed by a
:py:class:`~scalems.workflow.WorkflowManager`.
Workflows may be executed through different means and with different resources
through distinct modules. Different middleware implementations may be accessed
directly, but we recommend selecting a management module when invoking Python
from the command line with the ``-m`` option.

See :doc:`invocation` for usage information.

See :py:mod:`scalems.invocation` for more about Execution Modules.

Entry point
===========

The entry point for a `scalems` workflow script is the function decorated with
`scalems.app`

.. autodecorator:: scalems.app

Basic functions
===============

Core Function implementations provided with the SCALE-MS package.

.. py:currentmodule:: scalems

.. autofunction:: executable

.. py:currentmodule:: scalems.commands

.. autofunction:: extend_sequence

.. autofunction:: logical_and

.. autofunction:: logical_not

Dynamic functions
=================

Dynamic functions generate operations during graph execution.

.. autofunction:: map

.. autofunction:: reduce

.. autofunction:: while_loop

.. autofunction:: poll

Data shaping functions
======================

Establish and manipulate data flow topology.

.. autofunction:: desequence

.. autofunction:: resequence

Helpers
=======

Tools for dynamically generating Functions.

.. autofunction:: function_wrapper

.. autofunction:: subgraph

Speculative functions
=====================

These functions are probably not explicitly necessary, or at least not
appropriate for the high level interface.

.. autofunction:: gather

.. autofunction:: scatter

.. py:function:: broadcast

.. py:function:: concatenate(iterable: Iterable[T]) -> T

    Equivalent to ``reduce(extend_sequence, iterable)``

.. py:function:: partial

    Provide an alternative to :py:func:`functools.partial` that plays well with
    SCALE-MS checkpointing and dispatched execution.

Base classes
============

.. py:class:: Subgraph

    Base class with which to define Functions in terms of sub-graphs.

    Proposed alternative to the subgraph-builder context manager provided by
    subgraph().

    Example::

        # Create a subgraph Function with several Variables.
        #
        # * *simulation* names an input/output Variable.
        # * *conformation* names an output Variable.
        # * *P* names an internal state and output Variable.
        # * *is_converged* names an output Variable.
        #
        class MyFusedOperation(Subgraph):
            # The Subgraph metaclass applies special handling to these class variables
            # because of their type.
            simulation = Subgraph.InputOutputVariable(simulate)
            conformation = Subgraph.OutputVariable(default=simulation.conformation)
            P = Subgraph.OutputVariable(default=scalems.float(0., shape=(N, N)))
            is_converged = Subgraph.OutputVariable(default=False)

            # Update the simulation input at the beginning of an iteration.
            simulation.update(modify_input(input=simulation, conformation=conformation))

            # The Subgraph metaclass will hide these variables from clients.
            md = simulate(input=simulation)
            allframes = scalems.concatenate(md.trajectory)
            adaptive_msm = analysis.msm_analyzer(allframes, P)

            # Update Variables at the end of an iteration.
            simulation.update(md)
            P.update(adaptive_msm.transition_matrix)
            conformation.update(adaptive_msm.conformation)
            is_converged.update(adaptive_msm.is_converged)

            # That's all. The class body defined here is passed to the Subgraph
            # metaclass to generate the actual class definition, which will be
            # a SCALE-MS compatible Function that supports a (hidden) iteration
            # protocol, accessible with the `while_loop` dynamic Function.

        loop = scalems.while_loop(function=MyFusedOperation,
                                  condition=scalems.logical_not(MyFusedOperation.is_converged),
                                  simulation=initial_input)
        loop.run()

.. I list the members explicitly because nothing else seems to suppress the documentation of __fspath__ in Sphinx 4.1.2
.. autoclass:: scalems.context._file.FileReference
    :members: is_local, path, as_uri, localize

Logging
=======

.. automodule:: scalems.logger

Exceptions
==========

.. automodule:: scalems.exceptions
    :members:

.. autoclass:: scalems.context._file.DataLocalizationError
