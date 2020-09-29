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
               conformation = ['md.gro'],
               wrapper_name = 'gmx'):

    preprocess = scalems.commandline_operation(wrapper_name, 'grompp',
                                               input_files={
                                                   '-f': simulation_parameters,
                                                   '-p': topology,
                                                   '-c': conformation},
                                               output_files={
                                                   '-o': scalems.OutputFile(suffix='.tpr')
                                               })

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
