#In this example, we use a Python data flow scripting interface to connect
#PyEmma, a simulator adapter, and some additional Python code to iteratively
#run and analyze groups of simulations until convergence.
#
#supporting modules (`gromacs_wrapper.py` and `lammps_wrapper.py`) 
#wraps command line MD commands with a SCALE-MS wrapper layer.  The results should be in the same format.

from gromacs_wrapper import collect_configurations, internal_to_pdb, make_input, simulate, modify_input
from pyemma_msm import MSMAnalyzer

# configuration_list (an array of filenames) and md_inputs (a file
# name, or list of file names if multiple inputs are needed to
# initialize the simulation.

# these lines are a BIT GROMACS specific, as other programs may have slightly different formats.
coordinate_inputs = configuration_input_list # some list of files of length
run_parameters = md_inputs # some list of files
topology_files = topology_input_list # some list of files. 

num_simulations = 1000

initial_simulation_input = make_input(
    simulation_parameters=run_parameters,
    topology=topology_file,
    conformation=coordinate_inputs)

# Set up an array of num_simulations simulations, starting from a single input.

# see #28 for broadcast definitions.
initial_input = scalems.broadcast(initial_simulation_input, shape=(num_simulations,))

# We will need a pdb for MSM building in PyEmma
initial_pdb = internal_to_pdb(initial_input.get_coordinate(0))

tol = 0.1

def relative_entropy(P, Q):
    """
    Takes two transition matrices, calculates relative entropy
    """
    # Implementation incomplete
    return rel_entropy_P_Q


# Assuming MSMAnalyzer is an existing tool we do not want to modify,
# create a scalems compatible operation by wrapping with a provided utility.

msm_analyzer = scalems.make_operation(MSMAnalyzer,
                                      inputs=['topfile', 'trajectory', 'transition_matrix', 'num_clusters'],
                                      output=['is_converged', 'transition_matrix']
                                      )


simulation_and_analysis_iteration = scalems.subgraph(variables={
        'conformation': initial_input,
        'transition_matrix': scalems.ndarray(0., shape=(num_clusters, num_clusters)),  
        'is_converged': False})

# see PR #28, python.rst subgraph class definition
with simulation_and_analysis_iteration:
    modified_input = modify_input(
        input=initial_input, structure=simulation_and_analysis_iteration.conformation)
    md = simulate(input=modified_input)
    
    # Get the output trajectories and pass to PyEmma to build the MSM
    # Return a stop condition object that can be used in gmx while loop to
    # terminate the simulation
    allframes = collect_configurations(md.output.trajectory)
    
    adaptive_msm = analysis.msm_analyzer(molecular_topology=initial_pdb,
                                         trajectory=allframes,
                                         transition_matrix=subgraph.transition_matrix)

    # Update the persistent data for the subgraph
    simulation_and_analysis_iteration.transition_matrix = adaptive_msm.output.transition_matrix

    # adaptive_msm here is responsible for maintaining the ensemble width
    simulation_and_analysis_iteration.conformation = adaptive_msm.output.conformation
    simulation_and_analysis_iteration.is_converged = adaptive_msm.output.is_converged

    # In the default work graph, add a node that depends on `condition` and
    # wraps subgraph.

my_loop = gmx.while_loop(operation=simulation_and_analysis_iteration,
                             condition=scalems.logical_not(subgraph.is_converged))
my_loop.run()


