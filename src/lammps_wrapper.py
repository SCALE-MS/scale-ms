# Simulation preparation and output
# manipulation use command line tools. Simulation is executed with gmxapi.
#
# The idea would be that all wrappers return similar objects, so that they could be used 
# interchangeably by the rest of the tools.
#
# All wrappers should have, to the extent possible, the same methods.

import scalems

# NEED TO REWRITE FOR LAMMPS SYNTAX
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
    # strongly - a simulation tool set has some function that creates an object that encapsulates 
    # modify that input object, and convert the simulation to back into a simulation input object.
    # the pattern holds. 
    # key thing is that the overall program flow.  You package the inputs and pass it to the next tools. 

    # what we have been doing with gmxapi. 
    # 'Make input' command was consuming one file.  
    #
    # add features that if it is expecting 1 tpr and you give it 10
    # tpr files.  it creates a simulation input object that has
    # dynamic shape if you use it with other gmxapi components.  Those
    # components understand what it's shape is once you have created
    # an API object.  Although it supports array indexing, it is not a
    # tuple or a list, and should be recognized as an abstraction.

    return lammps_something(preprocess.output.files['-o'])

def internal_to_pdb(structure):
    editconf = scalems.commandline_operation(lammps_binary,
                                             input_files = {'-f': structure},
                                             output_files = {'-o': scalems.OutputFile(suffix='.pdb')})
    return editconf.output.files['-o']

def collect_coordinates(trajectory_files):
    allframes = scalems.commandline_operation(lammps_binary,
                                              input_files={'-f': scalems.gather(trajectories)},
                                              output_files={'-o': scalems.OutputFile(suffix='.trr')})
    return allframes.output.file['-o']

def simulate():

    # wraps a command line call of lammps

# a single modify commands but an array of modifiers.  If replacing some aspect of contents. 
# ensemble object.
def modify_input():
    
    # wraps loading a laammps input file, modifying it, and writing it back out.

