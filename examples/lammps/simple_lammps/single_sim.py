"""Set up and run a simple lammps simulation.

Make sure Gromacs 2021 and gmxapi 0.2 are already installed.

Then::

    $ python -m scalems.local single_sim.py
"""
import argparse
import logging
import os
from pathlib import Path

import scalems
from scalems.wrappers.lammps import make_input, simulate

# Allow log level to be set from the command line.
parser = argparse.ArgumentParser('Run a single simulation.')
parser.add_argument('--log', type=str, default='INFO',
                    help='Log level for the logging module.')
loglevel = parser.parse_args().log.upper()
numeric_level = getattr(logging, loglevel, None)
if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=numeric_level)
# End logging details...

lammps_binary = '/Users/mrshirts/lammps/lammps_2014/bin/lmp_mac'
# Locate the test files. Assumes the scalems repo was cloned recursively, or that
# the test data repository is otherwise available at dirname(thisfile)/../../testdata
script_dir = Path(__file__).parent.absolute()
data_dir = script_dir / 'lammps_inputs'
if not os.path.exists(data_dir):
    raise RuntimeError('Missing data.')


# Specify some input files.
# Note the input file names we are getting from the testdata repo.
# LAMMPS input file

# Could start with a list of distinct confs, but here we use a single starting point.
initial_structure = os.path.join(data_dir,'lj_bulk.lmp')  # GROMACS structure file.
                                 
simulation_parameters = os.path.join(data_dir,'lj_bulk.input')
included_inputs = [initial_structure]
input_type = 'file'
#input_type = 'commands'

# Define the entry point to the script.
@scalems.app
def main():
    if input_type == 'file':
        # run with file input.
        sim_input = make_input(
            simulation_parameters = simulation_parameters,
            included_inputs = included_inputs, return_file = 'commands.lammps')
        simulate(lammps_binary,os.path.join(script_dir,sim_input), input_type=input_type)
    elif input_type == 'commands':
        # run with a list of commands
        sim_input = make_input(
            simulation_parameters = simulation_parameters,
            included_inputs = included_inputs)
        simulate(lammps_binary,sim_input,input_type=input_type)
    else:
        raise RuntimeError(f'imput_type={input_type} is not supported')
        
