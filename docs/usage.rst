==============
Usage patterns
==============

Use Case Diagrams and general patterns of interaction with SCALE-MS or
collaborating software.

Application user
================

A human uses an application built on top of the Python data flow client API.

Script execution
================

A data flow script is executed to produce work that may be dynamically scheduled
and executed in terms of importable Python code.

Execution of dynamic work
=========================

An abstract work graph guides the generation and placement of tasks and input
data, informed by the availability of operation results that satisfy task data
dependencies or client requests.

.. _user classification:

User classes and characteristics
================================

Human users are assumed to be molecular science researchers using Python scripts to
express and execute simulation and analysis work consisting of multiple
simulation and analysis tasks, using software tools from multiple packages.
Non-human users may be :term:`script` s or any software participant interacting
with a part of the SCALE-MS :term:`framework`. If the term *user* does not seem
right to you, please consider substituting *role* in the following section.

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

.. seealso:: `Discussion <https://github.com/SCALE-MS/scale-ms/pull/27/files#r385761509>`_
    of a potentially more basic user.

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
