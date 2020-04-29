import gmxapi
import scalems


def make_input(simulation_parameters, topology, conformation):
    preprocess = scalems.commandline_operation('gmx', 'grompp',
                                               input_files={
                                                   '-f': run_parameters,
                                                   '-p': topology_file,
                                                   '-c': starting_structure},
                                               output_files={
                                                   '-o': scalems.OutputFile(suffix='.tpr')
                                               })
    return gmxapi.read_tpr(preprocess.output.files['-o'])


def gro_to_pdb(structure):
    editconf = scalems.commandline_operation('gmx', 'editconf',
                                             input_files={'-f': structure},
                                             output_files={'-o': scalems.OutputFile(suffix='.pdb')})
    return editconf.output.files['-o']


def collect_samples(trajectories):
    allframes = scalems.commandline_operation('gmx', 'trajcat',
                                              input_files={'-f': scalems.gather(trajectories)},
                                              output_files={'-o': scalems.OutputFile(suffix='.trr')})
    return allframes.output.file['-o']
