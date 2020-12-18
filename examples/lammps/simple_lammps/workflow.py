"""
Simple example of restarting a workflow.

This script provides an entry point for a workflow implemented in terms of
SCALE-MS data flow and execution management, plus user-provided support tools.

In this example, we use a Python data flow scripting interface to connect
PyEmma, a simulator adapter, and some additional Python code to iteratively
run and analyze groups of simulations until convergence.
"""

import scalems
import trajectory_continuer

# Get a set of simulation tools following a common interface convention.
from scalems.wrappers.lammps import collect_coordinates, internal_to_pdb, make_input, modify_input, simulate

# LAMMPS input file
# Could start with a list of distinct confs, but here we use a single starting point.
starting_structure = 'lammps_inputs/lj_bulk.input'  # GROMACS structure file.

simulation_parameters = ['lj_bulk.in']
included_inputs = ['lj_bulk.lmp']
initial_simulation_input = make_input(
    simulation_parameters = simulation_parameters,
    included_inputs = ['lj_bulk.lmp'])

# Set up a parallel pipeline of N=10 simulations, starting from a single input.
num_simulations = 2
# A `broadcast` function is possible, but is not explicitly necessary,
# so we have not specified one yet. See Python reference for discussion.
#    initial_input = scalems.broadcast(initial_simulation_input, shape=(N,))
initial_input = [initial_simulation_input] * num_simulations

# Get a placeholder object that can serve as a sub context / work graph owner
# and can be used in a control operation.
simulation_and_analysis_iteration = scalems.subgraph(variables={
    'conformation': initial_input,
    'iteration': 0,
    'is_complete': False})

with simulation_and_analysis_iteration:
    modified_input = modify_input(
        input=initial_input, structure=continue_structures
    )
    md = simulate(input=modified_input)

    # Get the output trajectories and pass to PyEmma to build the MSM
    # Return a stop condition object that can be used in gmx while loop to
    # terminate the simulation
    all_frames = collect_coordinates(md.output.trajectory)

    continue_structures = trajectory_continuer(trajectory=all_frames)
    trajectory_continuer.increment_iteration()

# In the default work graph, add a node that depends on `condition` and
# wraps subgraph.
my_loop = scalems.while_loop(function=simulation_and_analysis_iteration,
                             condition=scalems.logical_not(simulation_and_analysis_iteration.is_complete))
my_loop.run()
