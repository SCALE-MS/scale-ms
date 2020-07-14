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

# Set up a parallel pipeline of N=100 simulations, starting from a single input.
num_simulations = 100
seeds = list(range(num_simulations))
parameter_list = list()
for seed in range(num_simulations):
    parameter_list.append({'ld_seed': seed})
initial_input = modify_input(input=initial_simulation_input,
                              parameters=parameter_list)

# The num_simulations should equal or exceed the amount of concurrency available
# from the requested computing resources. Results will be processed in smaller
# batches. Batch sizes should be large enough that analysis tasks are not
# dominated by scheduling, event handling / data placement latency, or other
# synchronization, but small enough to minimize the cost of node failures and
# work heterogeneity (e.g. maximize occupancy).
batch_size = num_simulations // 10

async def batch_analysis(model, conformation: Iterable):
    """Return an updated model after applying a batch updated."""
    updated_model = model.update(await conformation)
    return updated_model


async def analysis_iteration(model, trajectories):
    """Perform an iteration on a wave of simulation results.

    Given an iterable of results, decompose into chunks for dispatching analysis
    tasks. Dispatch batch_size results at a time, as they become available.
    Use the analysis results to prepare the asynchronous iterable for the next
    wave of simulation.
    """
    simulation_input_set = set()
    # The model needs to be updated in a well-determined sequence, but we can
    # manage that entirely in terms of Futures by getting a new reference for
    # each future updated state of the model.
    for batch in scalems.as_completed(trajectories, batch_size=batch_size):
        model = batch_analysis(model, batch)
        # Note that the batch_analysis coroutine will await simulation outputs,
        # but since we have not awaited the coroutine yet, it is not yet a
        # blocking task. Add a future from the result to a set of awaitables.
        simulation_input_set.add(model.conformations)
    # Return a tuple that includes
    #  1. the reference to the model after the final batch update has been applied, and
    #  2. the set of awaitable simulation inputs.
    return (model, simulation_input_set)


async def simulation_iteration_optionA(inputs: Iterable[Future[SimulationInput]],
                                       conformations: Iterable[Future[SimulationFrame]]):
    """Perform an iteration of launching a wave of simulations.

    Given an asynchronous iterable of simulation inputs, launch simulations as
    the inputs become available. The set of simulation results compose the
    asynchronously awaitable output.
    """
    # Return a set of awaitable simulations.
    return {simulate(modify_input(i, conformation=x)) for i, x in zip(inputs, conformations)}


async def simulation_iteration_optionB(inputs: Iterable[Future[SimulationInput]]):
    """Perform an iteration of launching a wave of simulations.

    Given an asynchronous iterable of simulation inputs, launch simulations as
    the inputs become available. The set of simulation results compose the
    asynchronously awaitable output.
    """
    # Return a set of awaitable simulations.
    return {simulate(i) for i in inputs}


async def simulation_and_analysis_loop(initial_input):
    simulation_input = initial_input
    next_conformation = simulation_input.conformation
    model = msmtool.msm_analyzer()
    while not model.is_converged():
        # Option A would combine phase 0 and 1. E.g.:
        #     simulation = simulation_iteration_optionA(simulation_input, next_conformation)
        # Option B:
        # Phase 0: Prepare simulation input with the next round of initial conformations.
        simulation_input = modify_input(simulation_input, conformation=next_conformation)
        # Phase 1: Perform simulations
        simulation = simulation_iteration_optionB(simulation_input)
        # Phase 2: Update MSM to obtain next set of most informative initial conformations.
        model, next_conformation = analysis_iteration(model, simulation)
    return model


model = asyncio.run(simulation_and_analysis_loop(initial_input))