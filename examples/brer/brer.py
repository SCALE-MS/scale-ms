"""
Biased restrained ensemble refinement: New DEER incorporation workflow in gmxapi
"""

import json

# The researcher has written a brer_tools package to support their new method.
import brer_tools
# MD extension code, written in C++ has been compiled and installed for use as a plugin.
import myplugin  # Custom potentials

import scalems
from scalems.wrappers.gromacs import make_input
from scalems.wrappers.gromacs import modify_input
from scalems.wrappers.gromacs import simulate

# Add a TPR-loading operation to the default work graph (initially empty) that
# produces simulation input data bundle (parameters, structure, topology)

N = 50  # Number of ensemble members
starting_structure = 'input_conf.gro'
topology_file = 'input.top'
run_parameters = 'params.mdp'
potential_parameters = 'myparams.json'
with open(potential_parameters, mode='r') as fh:
    my_dict_params = json.load(fh)

# make a single simulation input file
initial_tpr = make_input(
        simulation_parameters=run_parameters,
        topology=topology_file,
        conformation=starting_structure
    )

initial_input = list([initial_tpr for _ in range(N)])  # An array of N simulations

# Just to demonstrate functionality, modify a parameter here.
# Change parameters with width 1 value. Implicitly broadcasts on an ensemble with width 50.
# If we wanted each ensemble member to have a different value, we would just
# have the new parameter value be an array of width 50, assuming API integration
# with the tool to understand allowed input data type and shape.
lengthened_input = modify_input(
    initial_input, parameters={'nsteps': 50000000})

# Create subgraph objects that encapsulate multiple operations
# and can be used in conditional and loop operations.
# For subgraphs, inputs can be accessed as variables and are copied to the next
# iteration (not typical for operation input/output).
train = scalems.subgraph(variables={'conformation': initial_input})

# References to the results of operations know which (sub)graph they live in.
# The `with` block activates and deactivates the scope of the subgraph in order
# to constrain the section of this script in which references are valid.
# Otherwise, a user could mistakenly use a reference that only points to the
# result of the first iteration of a "while" loop. If the `with` block succeeds,
# then the outputs of `train` are afterwards fully specified.
with train:
    myplugin.training_restraint(
        label='training_potential',
        params=my_dict_params)
    modified_input = modify_input(
        input=initial_input, structure=train.conformation)
    md = simulate(input=modified_input, potential=train.training_potential)
    # Alternative syntax to facilitate adding multiple potentials:
    # md.interface.potential.add(train.training_potential)
    brer_tools.training_analyzer(
        label='is_converged',
        params=train.training_potential.alpha)
    train.conformation = md.conformation
# At the end of the `with` block, `train` is no longer the active graph, and
# scalems.exceptions.ScopeError will be raised if `modified_input`, or `md` are used
# in other graph scopes (without first reassigning, of course)
# More discussion at https://github.com/kassonlab/gmxapi/issues/205

# In the default work graph, add a node that depends on `condition` and
# wraps subgraph.
train_loop = scalems.while_loop(
    function=train,
    condition=scalems.logical_not(train.is_converged))

# in this particular application, we "roll back" to the initial input
converge = scalems.subgraph(variables={'conformation': initial_input})

with converge:
    modified_input = modify_input(
        input=initial_input, structure=converge.conformation)
    myplugin.converge_restraint(
        label='converging_potential',
        params=train_loop.training_potential)
    brer_tools.converge_analyzer(
        converge.converging_potential.distances,
        label='is_converged',
    )
    md = simulate(input=modified_input, potential=converge.converging_potential)

conv_loop = scalems.while_loop(
    function=converge,
    condition=scalems.logical_not(converge.is_converged))

production_input = modify_input(
    input=initial_input, structure=converge.conformation)
prod_potential = myplugin.production_restraint(
    params=converge.converging_potential)
prod_md = simulate(input=production_input, potential=prod_potential)

prod_md.run()

print('Final alpha value was {}'.format(
    train_loop.training_potential.alpha.result()))
# also can extract conformation filenames, etc.
