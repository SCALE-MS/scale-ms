Adaptive Markov State Modeling: asynchronous
============================================

Note: may or may not have looping. Consider that MSM model builder outputs include
the convergence condition helpers and the simulation input emitters.


As in :doc:`parallel_adaptive_msm`,
we use a Python data flow scripting interface to connect
PyEmma, a simulator adapter, and some additional Python code to iteratively
run and analyze groups of simulations until convergence.

In this example application, we consider use cases in which many independent
simulations can be launched and processed asynchronously.
As data becomes available, the model is updated and the next simulations are
configured to improve the sampling needed to further refine the model.

Consider a few different modes of resource management and of workflow expression.

In the simplest case, the user may declare a number of simulations to run and
analyze the results as they are available (Example 1).

Adaptive work updates the requested simulations with repeated updates to the
model. This could be expressed in terms of a loop over individual analysis/update
tasks updating the collection of simulation futures (Example 2) or as an asynchronous loop
over batches or waves of simulation and analysis (Example 3).

Example 1: Unordered mapping
----------------------------

Simulation results are processed as they become available. This allows flexible
scheduling of analysis tasks as simulation tasks complete.

Adaptive variants include analysis driven exit conditions (e.g. running until
a convergence condition, rather than for a fixed number of simulations), but the
runtime must support some adaptive functionality in that the mapping of task
output to task input is not known until data is produced during execution.

:file:`examples/async_adaptive_msm/unordered_mapping.py`

.. literalinclude:: ../../../examples/async_adaptive_msm/unordered_mapping.py


Example 2: Data queue
---------------------

Simulation results are popped from an asynchronous queue, analyzed, and used to
inform the simulations added back to the queue.

:file:`examples/async_adaptive_msm/async_queue.py`

.. literalinclude:: ../../../examples/async_adaptive_msm/async_queue.py

Example 3: Asynchronous batches
-------------------------------

A batch of simulations is launched. Through an unordered mapping, the results
are processed as they become available, and the analysis results inform the
configuration of the next batch of simulations. An asynchronous looping construct
allows the next batch to be scheduled and launched opportunistically as inputs
dependencies are satisfied.

:file:`examples/async_adaptive_msm/async_iteration.py`

.. literalinclude:: ../../../examples/async_adaptive_msm/async_iteration.py
