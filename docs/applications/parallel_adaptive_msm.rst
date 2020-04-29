Adaptive Markov State Modeling
==============================

In this example, we use a Python data flow scripting interface to connect
PyEmma, a simulator adapter, and some additional Python code to iteratively
run and analyze groups of simulations until convergence.

The main script body defines and executes the subgraph.
There is obvious opportunity for abstraction, but it may be illustrative in the
present case to mix implementation details with algorithmic structure,
in part to stimulate discussion and understanding of packaging and component
capabilities.

One supporting module (:file:`examples/parallel_adaptive_msm/analyze.py`) wraps PyEmma.

Another supporting module (:file:`gromacsmd.py`) wraps GROMACS command line tools and gmxapi.

Main script body from :file:`examples/parallel_adaptive_msm/workflow.py`:

.. literalinclude:: ../../examples/parallel_adaptive_msm/workflow.py

The algorithmic details of the above scriplet depend on two supporting modules,
given here for completeness.

:file:`examples/parallel_adaptive_msm/analyze.py`:

.. literalinclude:: ../../examples/parallel_adaptive_msm/analyze.py

Simulation preparation and output
manipulation use command line tools. Simulation is executed with gmxapi.

:file:`examples/parallel_adaptive_msm/gromacsmd.py`

.. literalinclude:: ../../examples/parallel_adaptive_msm/gromacsmd.py
