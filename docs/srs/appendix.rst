========
Appendix
========

.. _control flow:

Discussion: Control flow
========================

High level logical constructs have highly degenerate functional requirements.
Traditional looping constructs (*for*, *for each*, *while*) can often be
expressed in terms of each other, and complex conditionals can be decomposed
into sequences of simple conditionals.

The classically fundamental control logic of *if* and *goto* can be mapped to
elemental work flow cases that may be illustrative, but do not necessarily
represent the most effective pure features on which to build optimized work management.

.. rubric:: Elemental case: conditional execution

A task may have no work to perform once its inputs are resolved.

.. rubric:: Elemental case: generated task

A task is added based on the results of other tasks.


Consider the following logical cases.
Each exhibits limiting behavior(s) that provide opportunities for optimization
and/or expose limits on generality of solutions to other cases.
Examination should clarify necessary and sufficient core functional requirements.

.. rubric:: case A: "while" loop

A known task (**or** subgraph) may be executed a number of times that is unknowable to
the scheduler, *a priori*, with total run time from zero to infinity.

.. rubric:: case B: branch selection

One or more arbitrarily large sections of graph are (de)activated in terms of
a task result.

.. rubric:: case C: arbitrary task generation

A task is added to a running graph that is either drawn from an arbitrarily
large set of possible operations, or that performs an operation not present in
the earlier graph.