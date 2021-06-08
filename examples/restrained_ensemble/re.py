"""Run restrained-ensemble sampling and biasing workflow."""

import json

# MD extension code, written in C++ has been compiled and installed for use as a plugin.
import myplugin
# The researcher has written a restrained_md_analysis package (not included) to support their new method.
from restrained_md_analysis import calculate_js

import scalems
from scalems.wrappers.gromacs import make_input
from scalems.wrappers.gromacs import simulate

# The user has already built 20 input files in 20 directories for an ensemble of width 20.
N = 20
starting_structure = 'input_conf.gro'
topology_file = 'input.top'
run_parameters = 'params.mdp'

initial_tpr = make_input(simulation_parameters=run_parameters,
                         topology=topology_file,
                         conformation=starting_structure)

initial_input = [initial_tpr for _ in range(N)]  # An array of simulations

# The user has stored parameters for their plugin in JSON files...
with open('params1.json', 'r') as fh:
    restraint1_params = json.load(fh)
with open('params2.json', 'r') as fh:
    restraint2_params = json.load(fh)

# Demonstrate an alternative to the helper functions and Python context manager
# builders for Subgraphs. Class definitions have scoping rules that better match
# our intended subgraph specification behavior, and subclassing provides the
# hooks we need to manage the abstractions we want for describing future commands,
# labeling graph elements, and routing data flow.
class Converge(scalems.Subgraph):
    pair_distance1 = scalems.Subgraph.OutputVariable(
        default=scalems.float(0., shape=(restraint1_params['nbins'],)))
    pair_distance2 = scalems.Subgraph.OutputVariable(
        default=scalems.float(0., shape=(restraint2_params['nbins'],)))

    # ensemble_restraint is implemented using gmxapi ensemble allReduce operations
    # that do not need to be expressed in this procedural interface.
    ensemble_restraint_1 = myplugin.ensemble_restraint(
        params=restraint1_params,
        input={'pair_distance': pair_distance1})
    ensemble_restraint_2 = myplugin.ensemble_restraint(
        params=restraint2_params,
        input={'pair_distance': pair_distance2})

    md = simulate(initial_input)
    # Note: binding resources that are opaque at the high level interface level
    # needs further discussion. Alternative syntax may be to pass a "potential"
    # key word argument to simulate().
    md.interface.potential.add(ensemble_restraint_1)
    md.interface.potential.add(ensemble_restraint_2)

    # Compare the distribution from the current iteration to the experimental
    # data and look for a threshold of low J-S divergence
    # We perform the calculation using all of the ensemble data.
    js_1 = calculate_js(
        input={'params': restraint1_params,
               'simulation_distances': scalems.gather(ensemble_restraint_1.pair_distance)})
    js_2 = calculate_js(
        input={'params': restraint2_params,
               'simulation_distances': scalems.gather(ensemble_restraint_2.pair_distance)})
    is_converged = scalems.logical_and([js_1.is_converged, js_2.is_converged])

    pair_distance1 = ensemble_restraint_1.pair_distance
    pair_distance2 = ensemble_restraint_2.pair_distance

work = scalems.while_loop(function=Converge, condition=scalems.logical_not(Converge.is_converged))

# Command-line arguments for mdrun can be added to run as below.
# Settings for a 20 core HPC node. Use 18 threads for domain decomposition for pair potentials
# and the remaining 2 threads for PME electrostatics.
work.run(work, tmpi=20, grid=(3, 3, 2), ntomp_pme=1, npme=2, ntomp=1)
