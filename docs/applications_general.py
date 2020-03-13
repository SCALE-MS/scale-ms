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

from gromacs_wrapper import collect_configurations, structure_to_pdb, make_input, simulate, modify_input

# configuration_list (an array of filenames) and md_inputs (a file
# name, or list of file names if multiple inputs are needed to
# intitialize the simulation.
#
# In theory, this could be abstracted further as some sort of input bundle.
# that contains all information needed to star the simulation.
# 
# So if one has N simulations, you would start with a list of N input objects.
# Each object could be tagged as a package of a supported type (GROMACS, LAMMPS, NAMD),
# and wrapper code for each supported type knows how to unwrap it.
#
# for GROMACS, it would be a .tpr file (or internal represntation of
# the .tpr file) For LAMMPS, it's the array of input files, since
# LAMMPS directly reads that.

coordinate_inputs = configuration_input_list # some list of files
run_parameters = md_inputs # some list of files
topology_files = topology_input_list # some list of files

initial_simulation_input = make_input(
    simulation_parameters=run_parameters,
    topology=topology_file,
    conformation=coordinate_inputs)

# Set up an array of N simulations, starting from a single input.
# MRS: seems like some instructions would need to be given here, such as:
#       # which entries in the files to replaced by random numbers
#       # may need a temporary directory to store modified files.
#       # so maybe the broadcasating would be done in make_input

initial_input = scalems.broadcast(initial_simulation_input, shape=(N,))

# We will need a pdb for MSM building in PyEmma
initial_pdb = coordinate_inputs[0]

# Get a placeholder object that can serve as a sub context / work graph owner
# and can be used in a control operation.

# MRS: I'm not sold on using the name subgraph, because it doesn't
#      describe what it is a subgraph of. Can this be made more specific?

# MRS: can a name be used instead of 'P' below? 
#

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
    allframes = collect_configurations(md.output.trajectory)
    
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
    
    def __init__(self, topfile, trajectory, P, N):

        # Build markov model with PyEmma
        feat = coor.featurizer(topfile)  # MRS: should be using something more generalizable than this.
        X = coor.load(trajectory, feat)
        Y = coor.tica(X, dim=2).get_output()
        k_means = coor.cluster_kmeans(Y, k=N)
        centroids = get_centroids(k_means)

        M = msm.estimate_markov_model(kmeans.dtrajs, 100)
        
            # Q = n-1 transition matrix, P = n transition matrix
        Q = P
        self.P = M.get_transition_matrix()  # figure this out
        self._is_converged = relative_entropy(self.P, Q) < tol

    def is_converged(self):
        return self._is_converged
    
    def transition_matrix(self):
        return self.P

# Assuming MSMAnalyzer is an existing tool we do not want to modify,
# create a scalems compatible operation by wrapping with a provided utility.

#MRS: suggest names rather than 'P' and 'N'

msm_analyzer = scalems.make_operation(MSMAnalyzer,
                                      inputs=['topfile', 'trajectory', 'P', 'N'],
                                      output=['is_converged', 'transition_matrix']
                                      )

# Simulation preparation and output
# manipulation use command line tools. Simulation is executed with gmxapi.
#
# The idea would be that all wrappers return similar objects, so that they could be used 
# interchangeably by the rest of the tools.
#
# All wrappers should have, to the extent possible, the same methods.
#
# gromacs_wrapper.py:
#

import gmxapi
import scalems

# MRS: we probably want to make these parameters keywords parameters
# instead of required parameters to make it clear what files are
# needed for each command.

def make_input(simulation_parameters = ['md.mdp'], 
               topology = ['md.top'], 
               initial_conformation = ['md.gro'], 
               binary_location = 'gmx'):

#
# makes decision here: wheether 
#
    preprocess = scalems.commandline_operation(binary_location, 'grompp',
                                               input_files={
            '-f': run_parameters,
            '-p': topology_file,
            '-c': starting_structure},
                                               output_files={
            '-o': scalems.OutputFile(suffix='.tpr')
            })

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

def modify_input():
    
    # wraps gmxapi modify_input 


#
#lammps_wrapper.py:
#	
# MRS: I will start writing this.    
