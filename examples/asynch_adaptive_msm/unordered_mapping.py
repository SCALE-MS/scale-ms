"""
Markov State Model building.

This script provides an entry point for a workflow implemented in terms of
SCALE-MS data flow and execution management, plus user-provided support tools.

In this example, we use a Python data flow scripting interface to connect
PyEmma, a simulator adapter, and some additional Python code to iteratively
run and analyze groups of simulations until convergence.
"""

# Acquire simulation inputs and common supporting code.
from .workflow_input import *

# Set up a parallel pipeline, starting from a single input.
num_simulations = 1000
# A `broadcast` function is possible, but is not explicitly necessary,
# so we have not specified one yet. See Python reference for discussion.
#    initial_input = scalems.broadcast(initial_simulation_input, shape=(N,))
initial_input = [initial_simulation_input] * num_simulations


# Get the Future for the sequenced iterable of MD trajectories.
trajectory_sequence = simulate(input=initial_input).output.trajectory

# Convert the sequence Future to an unordered iterable Future.
trajectory_unordered_collection = scalems.desequence(trajectory_sequence)
# Note that the Python *asyncio* standard library module has an `as_completed`
# function that performs the behavior for which we use `desequence`. We could
# rename `desequence` to `as_completed`, or keep `desequence` as a lower-level
# data annotation helper and use `as_completed` for the normative user interface
# for use cases such as this.
#
# Implementation note: through the Resource slicing interface, a sequence Future
# can be decomposed to Futures of the sequence elements. Through the Context
# resource subscription interface, either the original Future or the decomposed
# Futures can be converted to individual resource subscriptions that can be
# re-expressed to the user as an unordered resource in the local Python context.
#
# Question: The *trajectory* output is actually an ensemble array of sequences
# of trajectory frames, which in turn contain sequences of coordinate data.
# How do we distinguish the outer from the inner dimensions most intuitively?
# Maybe, like analogous numpy operations, commands like `desequence` have an
# optional *axis* or *dimension* argument?

# Asynchronously flatten the trajectory frames into an asynchronous iterable.
allframes = collect_coordinates(trajectory_unordered_collection)

# It is conceivable that *msm_analyzer* is implemented to decompose its own work
# in cooperation with the task management framework, in which case
# `msmtool.msm_analyzer(topfile=initial_pdb, trajectory=allframes)` could be
# efficiently scheduled with non-blocking asynchronicity, but for simplicity
# and consistency with the other examples, we assume the simpler implementation
# that updates the N-clusters model with (batches of) input.

scalems.map(msmtool.msm_analyzer, allframes).run()
# Question: How do we express that the model data is accumulated to a single
# entity?
# Question: Should we use the *shape* argument to `map` to indicate the intended
# chunking of *allframes* data to process?
