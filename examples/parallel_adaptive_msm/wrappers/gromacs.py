"""
Gromacs simulation tools.

Preparation and output manipulation use command line tools.
Simulation is executed with gmxapi.
"""

import gmxapi
import scalems

# Declare the public interface of this wrapper module.
__all__ = ['make_input', 'internal_to_pdb', 'collect_coordinates', 'simulate', 'modify_input']

# Get an exportable 'simulate' function for this module.
simulate = gmxapi.mdrun

# Get an exportable 'modify_input' function for this module..
modify_input = gmxapi.modify_input

# Define the remaining functions for a normalized simulation tool interface.


def make_input(simulation_parameters = ['md.mdp'],
               topology = ['md.top'],
               initial_conformation = ['md.gro'],
               wrapper_name = 'gmx'):

    preprocess = scalems.commandline_operation(wrapper_name, 'grompp',
                                               input_files={
                                                   '-f': simulation_parameters,
                                                   '-p': topology,
                                                   '-c': initial_conformation},
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

    return gmxapi.read_tpr(preprocess.output.files['-o'])


def internal_to_pdb(structure):
    editconf = scalems.commandline_operation('gmx', 'editconf',
                                             input_files={'-f': structure},
                                             output_files={'-o': scalems.OutputFile(suffix='.pdb')})
    return editconf.output.files['-o']


def collect_coordinates(trajectories):
    allframes = scalems.commandline_operation('gmx', 'trajcat',
                                              input_files={'-f': scalems.gather(trajectories)},
                                              output_files={'-o': scalems.OutputFile(suffix='.trr')})
    return allframes.output.file['-o']
