"""Set up and run a simple Gromacs simulation.

Make sure Gromacs 2021 and gmxapi 0.2 are already installed.

Then::

    $ python -m scalems.local single_sim.py
"""
import argparse
import logging
import os
from pathlib import Path

import scalems
from scalems.wrappers.gromacs import make_input, simulate


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

# Locate the test files. Assumes the scalems repo was cloned recursively, or that
# the test data repository is otherwise available at dirname(thisfile)/../../testdata
script_dir = Path(__file__).parent.absolute()
data_dir = script_dir.parent.parent / 'testdata' / 'alanine-dipeptide'
if not os.path.exists(data_dir):
    raise RuntimeError('Missing data. Ref https://github.com/SCALE-MS/scale-ms#test-data-submodule')

# Specify some input files.
# Note the input file names we are getting from the testdata repo.
params_file = os.path.join(data_dir, 'grompp.mdp')
topology_file = os.path.join(data_dir, 'topol.top')
configuration = os.path.join(data_dir, 'equil0.gro')

# Define the entry point to the script.
@scalems.app
def main():
    sim_input = make_input(simulation_parameters=params_file,
                           topology=topology_file,
                           conformation=configuration)
    md = simulate(sim_input)
    # Indicate where to force dependency resolution. This is an opportunity for
    # user-provided exception handling and inspection.
    scalems.wait(md)
    # TODO: Update scalems.run() and scalems.wait() to make sure that the
    #  dispatching has completed before the above returns.
    # TODO: What is the return value of a scalems.app?
    #  Maybe a final staging directive?
