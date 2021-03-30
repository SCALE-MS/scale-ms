"""
Iterative adaptive Markov State Model refinement.

This script provides an entry point for a workflow implemented in terms of
SCALE-MS data flow and execution management, plus user-provided support tools.

In this example, we use a Python data flow scripting interface to connect
PyEmma, a simulator adapter, and some additional Python code to iteratively
run and analyze groups of simulations until convergence.
"""

# Acquire simulation inputs and common supporting code.
from .workflow_input import *

# Construct an initial set of exploratory simulations.
seeds = list(range(100))
parameter_list = list()
for seed in range(100):
    parameter_list.append({'ld_seed': seed})
modified_input = modify_input(input=initial_simulation_input,
                              parameters=parameter_list)
# Note: a generic implementation should support syntax like
# `parameters={'ld_seed': seed for seed in range(100)}`, but the example as
# written may be more readable, and requires less development on the data model
# to properly handle.


# Declare a Queue of simulation results and populate it with some initial
# exploratory simulations.
queue = scalems.Queue()
# Note that aspects of the Queue instance will depend on the nature of the
# execution environment, the implementation of the executor adapters, and, thus,
# the associated Context instance, but we can probably assume that sensible
# default values for such things as *maxsize* are possible in all implementations,
# and thus in the proxy implementation for the local Context.
scalems.map(queue.put, simulate(input=modified_input))
# This `map` could reasonably be written in terms of the more generic `map`,
# and is essentially equivalent to
#     for simulation in simulate(input=modified_input):
#         queue.put(simulation)


# Iteratively process batches of results from the queue, adding new simulations
# to the queue as analysis completes.

# Get a placeholder object that can serve as a sub context / work graph owner
# and can be used in a control operation.
analysis_iteration = scalems.subgraph(variables={
    'queue': queue,
    'transition_matrix': scalems.ndarray(0., shape=(num_clusters, num_clusters)),
    'is_converged': False})

with analysis_iteration:
    # Get the results from the next simulation.
    md = analysis_iteration.queue.get()
    # Note: we may not want to use the `get` interface when setting up
    # "coroutine futures". It is not clear how to implement a comparable
    # `task_done()` message, or how best to pop several results at once,
    # and the Queue itself may be a lower level detail that doesn't belong in
    # the generic use case.
    # We also should have a way to get multiple items at a time.
    # We might use either a `partial_map` or a *chunksize* argument to `map`.
    # Alternatively, consider the pattern of `asyncio.wait(return_when=...)`
    # that allows behavior to be specified by an argument.

    # Get the output trajectories and pass to PyEmma to build the MSM
    # Return a stop condition object that can be used in gmx while loop to
    # terminate the simulation
    allframes = collect_coordinates(md.trajectory)

    adaptive_msm = msmtool.msm_analyzer(topfile=initial_pdb,
                                        trajectory=allframes,
                                        transition_matrix=analysis_iteration.transition_matrix)

    # Update the persistent data for the subgraph
    analysis_iteration.transition_matrix = adaptive_msm.transition_matrix
    analysis_iteration.is_converged = adaptive_msm.is_converged

    # Finish the iteration with conditional logic (1) to avoid adding more
    # unnecessary simulations to the queue, and (2) to allow this subgraph to
    # be reused to drain the queue.
    with scalems.conditional(condition=scalems.logical_not(analysis_iteration.is_converged)):
        # Use the MSM update to generate simulation inputs for further refinement.
        modified_input = modify_input(
            input=initial_simulation_input, structure=adaptive_msm.conformation)
        scalems.map(queue.put, simulate(modified_input))

# In the default work graph, add a node that depends on `condition` and
# wraps subgraph.
my_loop = scalems.while_loop(function=analysis_iteration,
                             condition=scalems.logical_not(analysis_iteration.is_converged))
my_loop.run()



# (Optionally) Drain the queue.
scalems.while_loop(function=analysis_iteration,
    condition=scalems.logical_not(queue.empty)).run()

