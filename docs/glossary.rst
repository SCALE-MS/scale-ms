==================
Terms and concepts
==================

Concepts and terminology may overlap with existing technologies,
whereas the requirements specified here are intended to be agnostic to specific solutions.
This document assumes the following definitions of potentially overloaded key words.

.. glossary::

    command
        Places one or more operations into the work graph.
        The syntax of UI-level functions that instantiate operations is specified by
        the API, but can extend the syntax implied by the serialized representation
        of a node for flexibility and user-friendliness. May be a *factory* for
        an operation implementation.

    edge
        A graph edge represents a (data) dependency between operations.

    ensemble
        An ensemble (as used here) is a concept for grouping related work.
        Defining an ensemble can allow high level work to be defined more
        conveniently while allowing more efficient management of task and data
        placement.
        Ensemble operations (such as scatter, gather, broadcast, and
        reduce), may be optimized within a single ensemble execution session.

        The run time characteristics of an ensemble include the supported data
        flow topology and the computing resources to allocate for ensemble
        workers. When an ensemble scope is entered, the framework may collect
        or launch new workers to support the ensemble work. Workers may continue
        to receive additional tasks and data until the ensemble scope changes.
        The scope of the ensemble session is thus also constrained by the
        appropriateness of the allocated ensemble worker pool for the available
        work.

    framework
        SCALE-MS is a set of specifications, support packages, and software
        collaborations that provide a framework in which software tools are
        executed. The term "framework" is used to refer abstractly to the
        SCALE-MS software stack and to the facilities it coordinates,
        particularly in documentation contexts where it is appropriate to avoid
        details of software packaging or implementation.

    function
    operation implementation
        A well defined computational element or data transformation that can be used
        to add computational work to a graph managed by a Context. Inputs
        are strongly specified, and behavior for a given set of inputs is deterministic
        (within numerical stability). Outputs may not be well specified
        until inputs are bound (*e.g.* until an instance is created).

    graph state
        The aggregation of state for operations (nodes) and data (edges),
        constrained by directed
        acyclic data flow topology. Granularity is not yet fully determined, but
        state must account for completed and incomplete operations, and allow
        distinction between idle and currently executing nodes.

    operation
    operation instance
        A node in a work graph. Previously described as *WorkElement* or *node*.

        Data sources and specific instances of operations are represented as
        nodes in a work graph.

        Identity: A node is the uniquely identifiable representation of a
        :term:`function` instance, defined in terms of the inputs and
        specified behavior.

        Corollary: the definition of an operation (a node) is immutable once added to the graph.

        Finer points:

        * The outputs of an operation may be accessed and subscribed to at any time.
        * Internally, operations may be stateful. They have metadata associated with
          their degree of completion and, potentially, with references to other
          resources to describe intermediate or final results.

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

    run time
        .. todo:: Define the scope to be conveyed by the noun "run time".

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

    task
        The smallest divisible unit of work representable within the API.
        Generally an :term:`operation` or a portion of an operation that has
        been decomposed in terms of decoupled parallelism or execution checkpoint
        interval.

    work
    graph
    work graph
        A task or tasks, packaged for dispatching and execution.

        Work is described as a directed acyclic graph (DAG) of data flow (edges)
        and operations on the data (nodes).
        Work represents the computational products
        requested by a client, but may be an abstraction for lower level tasks,
        and the exact work load may not be determined until run time.

    worker
        The unit of allocation of runtime computing resources. Should not be
        relevant to the user, but may figure into the implementation details of
        task dispatching.

        A process (or group of processes) managed by the framework and able to
        execute SCALE-MS work as directed by an :doc:`executor <executor>`.

        Workers are configured and launched through the `CPI`. Data and requests
        for data are provided to the worker by its client. The worker is able
        to publish both final results and intermediate results for the work it
        is executing. More generally, the worker publishes updates to the work
        graph, which include state updates for existing graph nodes as well as
        new nodes. New nodes are necessary to hold static data, to describe
        operations dispatched in support of higher level operations, or to
        extend the work graph (such as in support of adaptive work flows).

        A worker may be launched to perform a single task, to perform a sequence
        of tasks, or to participate in an :term:`ensemble`.

    CPI
        .. todo:: Define CPI.

    Context
      Abstraction for the entity that maps work to a computing environment.
      Instances may be long-lived and participate in owning/managing work and
      data references.

    Session
      Abstraction for the entity representing work that is executing on resources
      allocated by an instance of a Context implementation. The Session is the
      scoped active state of a Context while computing resources are held.

.. topic:: *Context* and *Session*

    roughly map to terms like *Executor* and *Task* in some other frameworks.
    Distinctions relate to the lifetime of the :term:`Context` instance, and the fact that
    it owns both the work specification (including operation and data handles)
    and the computing resources.
    The :term:`Context` instance owns resources (on behalf of the client) that may
    otherwise be owned directly by the client, and so its lifetime must span all
    references to resources, operation handles, and data futures.

.. glossary::

    discovered task
        A task that has become runnable, but was not already scheduled.
        This term is intentionally vague as the requirements and constraints of
        work management are explored. The primary usage refers to a task that
        has been generated due to adaptations in the work flow. It may also
        apply to tasks that may be scheduled opportunistically, or simply to
        the change of state when a task's input dependencies have been met.

.. _user classification:

User classes and characteristics
--------------------------------

Users are assumed to be molecular science researchers using Python scripts to
express and execute simulation and analysis work consisting of multiple
simulation and analysis tasks, using software tools from multiple packages.

Software tools are individually accessible as Python modules or as command line
tools.

Computational work may require multiple invocations (multiple HPC jobs) to complete.

The following classes of user are not necessarily mutually exclusive.

.. glossary::

    basic user
        A researcher writing a Python script to control standard software.

    advanced user
        A researcher who needs to integrate custom code into the scripted work.

    pure Python user
        All software necessary for the work is importable as Python modules.

    mixed command line user
        Some software is only accessible to Python by wrapping a command line driven tool.

    compiled extension user
        Some software necessary for the work requires compilation and/or installation
        on the computing resource.

    direct user
    local user
        Work is executed directly in the process(es) launched by the user.
        Examples include a Python interpreter launched on the user's desktop or
        a script launched with :command:`mpiexec` in a terminal window.

    indirect user
    remote user
        A script run by the user dispatches serialized work through API-enabled
        middleware for deserialization and execution outside of the user's
        Python interpreter. In addition to remote execution systems, this class
        may include adapters to *container* systems or job queuing systems,
        whether or not the execution occurs on the same machine as the initial
        Python interpreter.

It is useful to define some classes of client code.
These roles are relevant in describing use cases,
or as the subjects of user stories.

.. glossary::

    iterative method
        Similar work is performed repeatedly. The method may greatly constrain
        the complexity of the data flow topology compared to an equal number of
        unrelated tasks. The sequence of tasks may be arbitrarily long or short,
        and may not be knowable when the work begins execution. Examples include
        *while* loops and *for* loops.

    adaptive method
        Work is modified in response to task results. Modifications generally
        amount to extending the :term:`work graph` but we should consider how
        best to express cases in which previously expressed work becomes
        unnecessary. An :term:`iterative method` may be considered a sub-class
        of *adaptive method* when the stop condition requires evaluation of
        other task output.

    ensemble method
        Typified by parallel edges in a :term:`work graph` or subgraphs
        containing multiple high level single-instruction-multiple-data sorts of
        operations. As a usage class, we are considering cases where :term:`tasks <task>`
        are not tightly coupled, though :term:`operations <operation>`
        may be loosely coupled, or otherwise asynchronous
        :term:`instances <operation instance>`
        may be interspersed with coupled / synchronous operations.
        An *ensemble method* includes work and data that may be decomposed for
        asynchronous execution. Notably, the same decomposition is highly likely to
        be applicable to later work.
