========================
Glossary and terminology
========================

Concepts and terminology may overlap with existing technologies,
whereas the requirements specified here are intended to be agnostic to specific solutions.
This document assumes the following definitions of potentially overloaded key words.

.. glossary::

    task
        The smallest divisible unit of work representable within the API.
        Generally an :term:`operation` or a portion of an operation that has
        been decomposed in terms of decoupled parallelism or execution checkpoint
        interval.

    work
        A task or tasks, packaged for dispatching and execution.

    worker
        The unit of allocation of runtime computing resources. Should not be
        relevant to the user, but may figure into the implementation details of
        task dispatching.

    graph
    work graph
        A directed acyclic graph of operation nodes connected by data flow edges.

    graph state
        The aggregation of state for nodes and edges, constrained by directed
        acyclic data flow topology. Granularity is not yet fully determined, but
        state must account for completed and incomplete operations, and allow
        distinction between idle and currently executing nodes.

These two terms are borrowed from TensorFlow:

.. glossary::

    Context
      Abstraction for the entity that maps work to a computing environment.

    Session
      Abstraction for the entity representing work that is executing on resources
      allocated by an instance of a Context implementation.

The above terms roughly map to terms like *Executor* and *Task* in other frameworks.
Distinctions relate to the lifetime of the :term:`Context` instance, and the fact that
it owns both the work specification (including operation and data handles)
and the computing resources.
The :term:`Context` instance owns resources (on behalf of the client) that may
otherwise be owned directly by the client, and so its lifetime must span all
references to resources, operation handles, and data futures.

.. glossary::

    operation
        A well defined computational element or data transformation that can be used
        to add computational work to a graph managed by a Context. Operation inputs
        are strongly specified, and behavior for a given set of inputs is deterministic
        (within numerical stability). Operation outputs may not be well specified
        until inputs are bound.

    operation instance
    operation reference
    operation handle
    element
    node
        A node in a work graph. Previously described as *WorkElement*.

    operation factory
    operation helper
        The syntax of UI-level functions that instantiate operations is specified by
        the API, but can extend the syntax implied by the serialized representation
        of a node for flexibility and user-friendliness.

    port
        Generic term for a named source, sink, resource, or binding hook on a node.

    resource
        Describes an API hook for an interaction mediated by a Context. Data flow
        is described as *immutable* resources (generally produced as Operation outputs)
        that can be consumed by binding to Operation inputs or by extracting as *results*
        from the API. Some interactions cannot be represented in terms of producers
        and subscribers of immutable data events: *Mutable* resources cannot be
        managed by the Context as data events and require different work scheduling
        policies that either (a) allows arbitrary (unscheduled) call-back through the API framework,
        (b) dispatch the mutable resource collaboration to another Context, or (c)
        allow operations to bind and interact with an interface not specified by the
        API or not known to the responsible Context implementation. Examples include
        the Context-provided *ensemble_reduce* functionality, the ensemble simulation
        signaling facility (by which extension code can terminate a simulation early),
        and the binding mechanism by which MD extension code can be attached to an
        *MD* operation as a plugin. The nature of a resource is indicated by the
        namespace of its *port* in the work record.

.. glossary::

    discovered task
        A task that has become runnable, but was not already scheduled.
        This term is intentionally vague as the requirements and constraints of
        work management are explored. The primary usage refers to a task that
        has been generated due to adaptations in the work flow. It may also
        apply to tasks that may be scheduled opportunistically, or simply to
        the change of state when a task's input dependencies have been met.

    simulation segment
    trajectory segment
        A sequence of molecular simulation iterations or frames produced
        deterministically (within numerical limits) under well-determined
        parameters. For the purposes of discussing checkpoint intervals or the
        minimum amount of work executed between API calls, it is useful to
        distinguish between full simulation trajectories and the irreducible
        unit of work supported by a simulation library. In the simplest API use
        cases, a simulation library does not interact with the API during
        production of a simulation segment, and allows for reinitialization
        between simulation segments. This allows for unambiguous labeling of the
        artifacts of a segment. Optimizations may focus on reducing overhead
        between successive simulation segments (minimizing reinitialization).
        Extensions may introduce abstractions for well-characterized non-constant
        parameters, such as time-varying lambda values, though such abstractions
        are not required in the API since the effect can be achieved through
        binding to a mutable resource (with details beyond the scope of the API)
        owned by another operation whose state and action is well characterized
        for the segment.

Also refer to the definitions at :ref:`user classification`.
