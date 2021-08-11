==================
Terms and concepts
==================

High level terms and concepts
=============================

This glossary section describes terms and concepts relevant to the SCALE-MS
high level API and applications developed against it.

.. Terms appearing in user documentation should be defined here. Before introducing
   a potentially confusing term here, consider whether it should be restricted to
   lower level or developer/design documentation. Consider avoiding such terms in
   user level documentation, or providing an annotation of its technical nature
   when providing a link to the lower-level details.

Components
----------

A SCALE-MS based tool kit involves multiple software components.
The SCALE-MS packages comprise several components that are usually not visible
to basic research users. This section describes distinct components and concepts
that may be referenced in SCALE-MS documentation.
Component visibility and relevance should be annotated.

.. glossary::
    :sorted:

    API
    data flow scripting interface
        Application programming interface. This is the software interface on
        which SCALE-MS based applications or scripts are written.

    CPI
        Capability provider interface.
        An API for interacting with the execution framework at run time
        (i.e. within a worker process *after* it has been launched). Allows
        execution side task insertion, etc.

    application
        Software written in terms of the SCALE-MS :term:`API`. May be limited
        to :term:`client` code, or may include extension software that uses the
        SCALE-MS :term:`API` and :term:`framework` to compose work for managed
        execution.

    client
    API client
        Software or human user of the :term:`API`.

    client package
        A software package that implements the SCALE-MS :term:`API`.

    executor
        The pilot agent role by which processes are launched in an execution
        environment.

    execution manager
        A (theoretical) :term:`middleware` component that mediates the translation
        between data driven work flows and schedulable (task driven) work loads.

    framework
        SCALE-MS is a set of specifications, support packages, and software
        collaborations that provide a framework in which software tools are
        executed. The term *framework* is used in this documentation to refer
        abstractly to the SCALE-MS software stack and to the facilities it coordinates,
        particularly in documentation contexts where it is appropriate to avoid
        details of software packaging or implementation. When multiple software
        frameworks are interacting, we should be more specific, but we may neglect
        to specify *SCALE-MS framework* when the context seems unambiguous.

    middleware
        The :term:`client package` dispatches :term:`work` for execution through
        a layer of abstraction that is logically separated from the :term:`run time`
        layer to allow flexible connection to very different computing resources,
        scheduling, and work management systems.

    run time
    runtime
        The software environment in which SCALE-MS :term:`task` s are executed.

    script
        Instructions issued to a Python interpreter session that includes import
        of the :term:`client package`. Note that, even if no :term:`API` instructions
        are issued, SCALE-MS software may be initialized during module import.
        The *script* is used to mean the scope of the :term:`client package`
        module activity.

    simulator
        Software called by SCALE-MS that can be instructed to carry out
        molecular simulation work.
        The simulator may be a separate executable (accessed through the command line),
        or a library (accessed through a Python, C++, or gRPC API).
        The SCALE-MS API abstracts these calls so that the user does not need to
        know how the simulator is being accessed.

    worker
        A logical unit of allocated of runtime computing resources:
        A process (or group of processes, such as spanning the ranks of an MPI context),
        managed (through the framework) by a run time system, able to
        execute SCALE-MS :term:`work`.

        Should not be relevant to the user,
        but may figure into the implementation details of task dispatching.

        Workers are configured and launched through the :term:`CPI`.
        Data and requests for data are provided to the worker by its CPI client.
        The worker is able to publish both final results and intermediate
        results for the work it is executing.
        More generally, the worker publishes updates to the task graph,
        which include state updates for existing graph nodes as well as
        new nodes.
        New nodes are necessary to hold static data,
        to describe operations dispatched in support of higher level operations,
        or to extend the work graph (such as in support of adaptive work flows).

        A worker may be launched to perform a single task, to perform a sequence
        of tasks, or to participate in an :term:`ensemble`.

    work flow
        A network of :term:`command` s or high level (abstract) tasks.

    work load
        A representation of work that is dispatched for managed execution,
        represented in SCALE-MS in terms of a task :term:`graph` or portion thereof.
        May be dynamic, with additional work added after dispatch.

.. todo:: Separate essential user-level definition from more in-depth discussion.

Interface terminology
---------------------

.. glossary::
    :sorted:

    command
        Places one or more high level tasks into the work graph.
        The syntax of UI-level functions that instantiate operations is specified by
        the API, but can extend the syntax implied by the serialized representation
        of a node for flexibility and user-friendliness. May be a *factory* for
        an operation implementation.

    function
    operation implementation
        A well defined computational element or data transformation that can be used
        to add computational :term:`work` to a :term:`graph`. The SCALE-MS
        :term:`client package` includes various built-in functions with which to
        connect external functions (imported from SCALE-MS compatible software
        packages) or custom user-provided code.

        The function implementation strongly specifies allowed inputs,
        and behavior for a given set of inputs is deterministic
        (within numerical stability). Outputs may not be well specified
        until inputs are bound
        (*e.g.* until an :term:`instance <operation instance>` is created).

    task
    operation
    operation instance
        Data sources and specific instances of :term:`function` s managed by the
        framework to fulfil a :term:`command`.

        *Tasks* can be thought of as nodes in a :term:`work graph`, but the
        nature of a *task* is a factor of the interface layer (and the level of
        the software stack). The scope of the term has varied over the project
        history, but the current sense is that communication is clearest if we
        allow the term to be used in various scopes, while emphasizing the
        context of the discussion with qualifiers. E.g. *runtime task* or
        *high level task*.

        Identity: An *task* is the uniquely identifiable representation of a
        :term:`function` instance, defined in terms of the inputs and
        specified behavior.

        Corollary: the definition of a task (a node) is immutable once added to the graph.

        Finer points:

        * The outputs of a task may be accessed and subscribed to at any time.
        * Internally, tasks may be stateful. They have metadata associated with
          their degree of completion and, potentially, with references to other
          assets (artifacts) to describe intermediate or final results.

Data flow terminology
---------------------

.. glossary::
    :sorted:

    edge
        A graph edge represents a (data) dependency between :term:`task` s.

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

    node
        We avoid this term, not just because it overloads vocabulary used to
        describe HPC computing environments, but because it is imprecise in the
        context of the SCALE-MS abstraction layers. Usually, some qualified form
        of the term :term:`task` is clearer. A truly abstract graph (encoding an
        algorithm as a network of API abstractions) may most properly map a node
        to a :term:`command`.

    port
        Generic term for a named source, sink, data resource, or binding hook on a
        :term:`function` or :term:`task`.
        :term:`task` s are represented as providing and consuming resources through named *ports*.
        See also: :term:`asset`.

    work
    graph
    work graph
        A prescription to produce computational results, packaged for dispatching and execution.

        Work is described as a directed acyclic graph (DAG) of data flow (:term:`edge` s)
        and operations on the data (:term:`task` s).
        Work represents the computational products
        requested by a client, but may be an abstraction for lower level tasks,
        and the exact work load may not be determined until run time.

Development and implementation
==============================

Concepts and terminology may overlap with existing technologies,
whereas the requirements specified here are intended to be agnostic to specific solutions.
This document assumes the following definitions of potentially overloaded key words.

.. glossary::

    graph state
        The aggregation of state for operations (nodes) and data (edges),
        constrained by directed
        acyclic data flow topology. Granularity is not yet fully determined, but
        state must account for completed and incomplete operations, and allow
        distinction between idle and currently executing nodes.

    computing resource
        Facility, resource, capability, or detail of a computing environment
        that must be allocated and accessed to dispatch and execute computational
        tasks. Not to be confused with :term:`API resource`.

    asset
    data resource
        Describes an API hook for an interaction mediated by a Context. Data flow
        is described as *immutable* resources (generally produced as :term:`operation` outputs)
        that can be consumed by binding to :term:`function` inputs (subscribing)
        or by extracting as *results* from the API.
        Some interactions cannot be represented in terms of producers
        and subscribers of immutable data events: *mutable* resources cannot be
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

        Not to be confused with :term:`computing resource`

    simulation segment
        The smallest amount of :term:`simulator` :term:`work` that is representable
        within SCALE-MS.
        Note that coupled simulation tasks represented in a high-level interface
        may be decomposed into sequences of uncoupled tasks for run time execution
        when the software bridge to the simulator can encode such segmentation.

        May correspond to a single API call, a check-point
        interval, a call-back interval, a trajectory output interval, or whatever
        unit of abstract simulation work is necessary to discuss the unit of
        identifiable and reproducible simulation output: a :term:`trajectory segment`.

    trajectory segment
        The smallest unit of reproducible :term:`simulator` output for a
        simulation :term:`operation`.
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

        .. Note: If there are sequences of simulation frames that cannot be meaningfully
            decomposed at the SCALE-MS level, then that is a segment.
            If several such segments are independently identifiable,
            but not individually meaningful because they are coupled by some ensemble method,
            then they are "coupled segments" or an "ensemble segment," I think.

    Context
        Abstraction for the software component that maps work to a computing environment.
        Instances may be long-lived and participate in owning/managing work and
        data references.

    Session
        Object or abstraction representing work that is executing on computing resources
        allocated according to an instance of a Context implementation.
        The Session is the (scoped) active state of a Context while computing
        resources are held. A `radical.pilot.Session` may provide the implementation
        of a Session in a RADICAL Pilot based Context.

..    discovered task
        A task that has become runnable, but was not already scheduled.
        This term is intentionally vague as the requirements and constraints of
        work management are explored. The primary usage refers to a task that
        has been generated due to adaptations in the work flow. It may also
        apply to tasks that may be scheduled opportunistically, or simply to
        the change of state when a task's input dependencies have been met.
