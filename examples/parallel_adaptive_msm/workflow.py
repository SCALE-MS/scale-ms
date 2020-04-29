import scalems

from gmxapi import modify_input
from gmxapi import mdrun as simulate

from gromacsmd import collect_samples, gro_to_pdb, make_input

# Could start with a list of distinct confs, but here we use a single starting point.
# GROMACS MD model and method parameters.
starting_structure = 'input_conf.gro'  # GROMACS structure file.
topology_file = 'input.top'  # GROMACS topology input.
run_parameters = 'params.mdp'  # GROMACS simulation parameters.

initial_simulation_input = make_input(
    simulation_parameters=run_parameters,
    topology=topology_file,
    conformation=starting_structure)

# Set up an array of N simulations, starting from a single input.
initial_input = scalems.broadcast(initial_simulation_input, shape=(N,))

# We will need a pdb for MSM building in PyEmma
initial_pdb = gro_to_pdb(starting_structure)

# Get a placeholder object that can serve as a sub context / work graph owner
# and can be used in a control operation.
subgraph = scalems.subgraph(variables={
    'conformation': initial_input,
    'P': scalems.ndarray(0., shape=(N, N)),
    'is_converged': False})

with subgraph:
    modified_input = modify_input(
        input=initial_input, structure=subgraph.conformation)
    md = simulate(input=modified_input)

    # Get the output trajectories and pass to PyEmma to build the MSM
    # Return a stop condition object that can be used in gmx while loop to
    # terminate the simulation
    allframes = collect_samples(md.output.trajectory)

    adaptive_msm = analysis.msm_analyzer(topfile=editconf.file['-o'],
                                         trajectory=allframes,
                                         P=subgraph.P)
    # Update the persistent data for the subgraph
    subgraph.P = adaptive_msm.output.transition_matrix
    # adaptive_msm here is responsible for maintaining the ensemble width
    subgraph.conformation = adaptive_msm.output.conformation
    subgraph.is_converged = adaptive_msm.output.is_converged

# In the default work graph, add a node that depends on `condition` and
# wraps subgraph.
my_loop = gmx.while_loop(operation=subgraph,
                         condition=scalems.logical_not(subgraph.is_converged))
my_loop.run()