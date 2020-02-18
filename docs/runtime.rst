============================
SCALE-MS run time interfaces
============================

.. glossary::

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

    graph
        Work is described as a directed acyclic graph (DAG) of data flow (edges)
        and operations on the data (nodes).

    node
        Data sources and specific instances of operations are represented as
        nodes in a work graph.

        Identity: A node is the uniquely identifiable representation of an
        operation instance, defined in terms of the inputs and
        the work they will perform.

        Corollary: the definition of a node is immutable once added to the graph.

        Finer points:

        * The outputs of a node may be accessed and subscribed to at any time.
        * Internally, nodes may be stateful. They have metadata associated with
          their degree of completion and, potentially, with references to other
          nodes to describe intermediate or final results.

    run time
        .. todo:: Define the scope to be conveyed by the noun "run time".

    worker
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

Initial implementation
======================

We should clarify the scopes of the CPI and the RPC protocol.
If they are implemented discretely, the following functionality may fall clearly
into one scope or the other.

* Create ensemble
    * Allocate workers or group existing workers (CPI)
    * Enter ensemble logical scope (declares data scope and graph state) (RPC)
* Destroy ensemble
    * Leave ensemble logical scope (ungroup workers) (RPC)
    * Allow worker execution session to end (independent of marking the ensemble scope of the work as complete) (CPI)
* Pass commands and data back and forth on gRPC interface
    * Command issuer may:
        * place nodes (operations, data, and metadata for separately place resources)
        * react to published results
        * Cancel an operation (such as when a worker failure makes continued execution undesirable).
    * Worker may:
        * Publish results (intermediate or final)
        * Declare completion (update graph node state)
* Challenges in check point recovery
    * How to decide what data needs to be re-placed (maybe already solved in RADICAL?)
    * Need to externally determine which nodes to place when resetting ensemble to a useful state from which to resume.
