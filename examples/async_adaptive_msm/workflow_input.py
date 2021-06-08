"""Hold the reusable part of the various workflows in this example."""

__all__ = ['scalems', 'initial_simulation_input', 'num_clusters', 'initial_pdb', 'collect_coordinates', 'msmtool',
           'simulate', 'modify_input']

import scalems
# Get a set of simulation tools following a common interface convention.
from scalems.wrappers.gromacs import internal_to_pdb
from scalems.wrappers.gromacs import make_input
# Get a set of simulation tools following a common interface convention.
from scalems.wrappers.gromacs import modify_input
from scalems.wrappers.gromacs import simulate
# Get a module that provides the `msm_analyzer` function we will be using.
from . import msmtool

# Generic data hierarchy is not yet clear. Model,
# method, instance/system/conformation/microstate, and implementation details
# will be reflected. The current example reflects the requirements of GROMACS
# simulation input. Specific input requirements are expressed in the interface
# to the `make_input` wrapper module function.

# GROMACS MD model and method parameters.
# Could start with a list of distinct confs, but here we use a single starting point.
starting_structure = 'input_conf.gro'  # GROMACS structure file.
topology_file = 'input.top'  # GROMACS topology input.
run_parameters = 'params.mdp'  # GROMACS simulation parameters.

initial_simulation_input = make_input(
    simulation_parameters=run_parameters,
    topology=topology_file,
    conformation=starting_structure)

# We will need a pdb for MSM building in PyEmma
initial_pdb = internal_to_pdb(starting_structure)

# Build a model with 10 Markov states (clusters in conformation space).
num_clusters = 10
