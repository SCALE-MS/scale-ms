#In this example, we use a Python data flow scripting interface to connect
#PyEmma, a simulator adapter, and some additional Python code to iteratively
#run and analyze groups of simulations until convergence.
#
#The main script body defines and executes the subgraph.
#
#One supporting module (`analyze.py`) wraps PyEmma.
#
#supporting modules (`gromacs_wrapper.py` and `lammps_wrapper.py`) 
#wraps command line MD commands with a SCALE-MS wrapper layer.  The results should be in the same format.

from gromacs_wrapper import collect_configurations, internal_to_pdb, make_input, simulate, modify_input

# configuration_list (an array of filenames) and md_inputs (a file
# name, or list of file names if multiple inputs are needed to
# intitialize the simulation.

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

# NOTE: we presume that broadcast creates num_simulations even if the initial simulation input contains
# fewer initial coordinates than num simulations. Like, if num_simulations is 40, and the initial 
# coordinate list contains 7, then it will iterate (1,2,3,4,5,6,7,1,2,3,4,5, ... until reaching 40)?
# and so on with the other files?

initial_input = scalems.broadcast(initial_simulation_input, shape=(num_simulations,))

# We will need a pdb for MSM building in PyEmma
initial_pdb = internal_to_pdb(initial_input.get_coordinate(0))

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

#
#analyze.py::
#

"""
Analysis tool for adaptive msms
"""

import pyemma
import pyemma.coor as coor
import pyemma.msm as msm

tol = 0.1

def relative_entropy(P, Q):
    """
    Takes two transition matrices, calculates relative entropy
    """
    # Implementation incomplete
    return rel_entropy_P_Q


class MSMAnalyzer:
    """
    Builds msm from output trajectory
    """
    
    def __init__(self, molecular_topology_file, trajectory, transition_matrix, num_clusters):

        # Build markov model with PyEmma
        feat = coor.featurizer(moleular_topology_file)  
        X = coor.load(trajectory, feat)
        Y = coor.tica(X, dim=2).get_output()
        k_means = coor.cluster_kmeans(Y, k=num_clusters)
        centroids = get_centroids(k_means)

        markov_model = msm.estimate_markov_model(kmeans.dtrajs, 100)  # 
        
        previous_transition_matrix = transition_matrix
        self.transition_matrix = markov_model.get_transition_matrix()  # figure this out
        self._is_converged = relative_entropy(self.transition_matrix, transition_matrix) < tol

    def is_converged(self):
        return self._is_converged
    
    def get_transition_matrix(self):
        return self.transition_matrix

# Assuming MSMAnalyzer is an existing tool we do not want to modify,
# create a scalems compatible operation by wrapping with a provided utility.

msm_analyzer = scalems.make_operation(MSMAnalyzer,
                                      inputs=['topfile', 'trajectory', 'transition_matrix', 'num_clusters'],
                                      output=['is_converged', 'transition_matrix']
                                      )

# Simulation preparation and output
# manipulation use command line tools. Simulation is executed with gmxapi.
#
# The idea would be that all wrappers return similar objects, so that they could be used 
# interchangeably by the rest of the tools.
#
# All wrappers should have, to the extent possible, the same methods.


# gromacs_wrapper.py:

import gmxapi
import scalems

def make_input(simulation_parameters = ['md.mdp'], 
               topology = ['md.top'], 
               initial_conformation = ['md.gro'], 
               binary_location = 'gmx'):


    preprocess = scalems.commandline_operation(binary_location, 'grompp',
                                               input_files={
            '-f': run_parameters,
            '-p': topology_file,
            '-c': starting_structure},
                                               output_files={
            '-o': scalems.OutputFile(suffix='.tpr')
            })

    # simulation object. Structured the same for all wrappers, but you can't use one from another.
    # strongly - a simulation toolset has some function that creates an object that encapsulates 
    # modify that input object, and convert the simulataon to back into a simulation inpput object.
    # the pattern holds. 
    # key thing is that the overall program flow.  You package the iniputs and pass it to the next tools. 

    # what we havee been doing with gmxapi. 
    # 'Make input' command was consuming one file.  
    #
    # add features that if it is expecting 1 tpr and you give it 10 tpr files.
    # it creates a simulatioin input object that has dynamic shape If you use it with other gmxapi components.
    # they understand what it's shape is.   And it does support array indexing. 
    # once you have createad aan API object. We want people to recognize it's an abstraction. 
    # should make assumptions about accessing as an array

    return gromacs_api.read_tpr(preprocess.output.files['-o'])

def internal_to_pdb(structure):
    editconf = scalems.commandline_operation('gmx', 'editconf',
                                             input_files = {'-f': structure},
                                             output_files = {'-o': scalems.OutputFile(suffix='.pdb')})
    return editconf.output.files['-o']

def collect_coordinates(trajectory_files):
    allframes = scalems.commandline_operation('gmx', 'trajcat',
                                              input_files={'-f': scalems.gather(trajectories)},
                                              output_files={'-o': scalems.OutputFile(suffix='.trr')})
    return allframes.output.file['-o']

def simulate():

    # wraps gmxapi mdrun.

# a single modify commands but an array of modifiers.  If replacing some aspect of contents. 
# ensemble obect.
def modify_input():
    
    # wraps gmxapi modify_input 

#
#lammps_wrapper.py:
#	
# MRS: I will start writing this.    
