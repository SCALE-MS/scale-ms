Introduction
============

Purpose
-------

The SCALE-MS project defines a framework in which computational molecular
science achieves new scalability by mapping complex research protocols to
large scale computing resources.

Document conventions
--------------------

This document uses a mixture of Python syntax and JSON syntax to represent
data structures.

Abstract syntax grammar may be described with
`BNF notation <https://www.w3.org/Notation.html>`__.

Concepts and terminology may overlap with existing technologies,
whereas the requirements specified here are intended to be agnostic to specific solutions.
Refer to the :doc:`glossary` for key terms and their intended meanings in the
context of this document.

Description
===========

Product perspective
-------------------

The scopes of several related efforts are still being clarified.

Initial data flow driven molecular simulation and analysis management tools
have been developed under the `gmxapi <http://gmxapi.org>`_ project,
now maintained with the `GROMACS <http://www.gromacs.org>`_ molecular simulation
package.

The `RADICAL <https://radical-cybertools.github.io/>`_ project has developed a
software stack that we hope to leverage.

Implementation details for SCALE-MS API functionality may require extension of
external tools, such as RADICAL or `Parsl <http://parsl-project.org/>`_.

Product functions/features
--------------------------

The API under development supports arbitrary complex and adaptive
computational molecular science research protocols on large and varied
computing resources.

Through its implementation library or integrations, the API must support

* Scriptability of multi-step multi-tool variably parallel computation and analysis.
* Data flow driven work, compartmentalized as nodes on a directed acyclic graph.
* Work specifications that are trivially portable to different computing environments.
* Full workflow checkpointing and optimal recoverability.
* Data abstractions that minimize the specification of artifact storage or transfer.
* Data locality optimizations that allow data transfers to be optimized or
  eliminated according to data dependencies.
* Data locality optimizations that allow work scheduling decisions to be made
  in terms of data dependencies.

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

Operating environment
---------------------

Computational work is performed in Unix-like environments including
high performance computing (HPC) centers and desktop workstations.

Software is built against CPython 3.5 or higher.

Tasks launched through the software may employ acceleration technologies
including but not limited to MPI, OpenMP, and GPUs.

User script processing may occur through various modes of Python interpreter invocation.
 * Interpreter binary launched from a shell, either directly on the command line
   or via shell script processing ("shebang" line)
 * Interactive Python interpreter
 * Jupyter notebook
 * Parallel launch through a wrapper such as ``mpiexec``, ``srun``, or ``aprun``.

.. admonition:: Question

    Should scripts be executable via ``mpiexec -n 2 `which python` -m mpi4py script.py``
    or should we disallow mixing of job management at the user interface?
    The alternative is to require adapters for all dispatching front-ends and/or
    to segregate the package that handles user scripts from the package that
    handles dispatching work to a concrete execution context.

Design and implementation constraints
-------------------------------------

User documentation
------------------

Assumptions and Dependencies
----------------------------
