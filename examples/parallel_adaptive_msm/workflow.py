"""
Iterative adaptive Markov State Model refinement.

This script provides an entry point for a workflow implemented in terms of
SCALE-MS data flow and execution management, plus user-provided support tools.

In this example, we use a Python data flow scripting interface to connect
PyEmma, a simulator adapter, and some additional Python code to iteratively
run and analyze groups of simulations until convergence.
"""

import scalems

# Get a module that provides the `msm_analyzer` function we will be using.
from . import msmtool

# Get a set of simulation tools following a common interface convention.
from scalems.wrappers.gromacs import collect_coordinates, internal_to_pdb, make_input, modify_input, simulate

# Acquire simulation inputs. Generic data hierarchy is not yet clear. Model,
# method, instance/system/conformation/microstate, and implementation details
# will be reflected. The current example reflects the requirements of GROMACS
# simulation input. Specific input requirements are expressed in the interface
# to the `make_input` wrapper module function.

# GROMACS MD model and method parameters.
# Could start with a list of distinct confs, but here we use a single starting point.
starting_structure = 'input_conf.gro'  # GROMACS structure file.
topology_file = 'input.top'  # GROMACS topology input.
run_parameters = 'params.mdp'  # GROMACS simulation parameters.

initial_simulation_input = make_input(
    simulation_parameters=run_parameters,
    topology=topology_file,
    conformation=starting_structure)

# Set up a parallel pipeline of N=10 simulations, starting from a single input.
num_simulations = 10
# A `broadcast` function is possible, but is not explicitly necessary,
# so we have not specified one yet. See Python reference for discussion.
#    initial_input = scalems.broadcast(initial_simulation_input, shape=(N,))
initial_input = [initial_simulation_input] * num_simulations

# We will need a pdb for MSM building in PyEmma
initial_pdb = internal_to_pdb(starting_structure)

# Build a model with 10 Markov states (clusters in conformation space).
num_clusters = 10

# Get a placeholder object that can serve as a sub context / work graph owner
# and can be used in a control operation.
simulation_and_analysis_iteration = scalems.subgraph(variables={
    'conformation': initial_input,
    'transition_matrix': scalems.ndarray(0., shape=(num_clusters, num_clusters)),
    'is_converged': False})

with simulation_and_analysis_iteration:
    modified_input = modify_input(
        input=initial_input, structure=simulation_and_analysis_iteration.conformation)
    md = simulate(input=modified_input)

    # Get the output trajectories and pass to PyEmma to build the MSM
    # Return a stop condition object that can be used in gmx while loop to
    # terminate the simulation
    allframes = collect_coordinates(md.output.trajectory)

    adaptive_msm = msmtool.msm_analyzer(topfile=initial_pdb,
                                        trajectory=allframes,
                                        transition_matrix=simulation_and_analysis_iteration.transition_matrix)
    # Update the persistent data for the subgraph
    simulation_and_analysis_iteration.transition_matrix = adaptive_msm.output.transition_matrix
    # adaptive_msm here is responsible for maintaining the ensemble width
    simulation_and_analysis_iteration.conformation = adaptive_msm.output.conformation
    simulation_and_analysis_iteration.is_converged = adaptive_msm.output.is_converged

# In the default work graph, add a node that depends on `condition` and
# wraps subgraph.
my_loop = scalems.while_loop(function=simulation_and_analysis_iteration,
                             condition=scalems.logical_not(simulation_and_analysis_iteration.is_converged))
my_loop.run()
