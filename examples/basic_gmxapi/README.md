# Basic gmxapi simulation

Simple gmxapi workflow to configure and run MD for an ADP molecule.

Assumes Gromacs 2021 and gmxapi 0.2 are already installed, as well as scalems.

For example usage, see comments in the [Dockerfile](Dockerfile) in this directory.

For a clean test environment pull and/or build a Docker image following instructions in the Dockerfile.

## Example 1

`single_sim.py` makes trivial use of the proof-of-concept code in the
[`src/scalems/wrappers/gromacs.py`](../../src/scalems/wrappers/gromacs.py) module,
which generates a chain of 5 tasks to support the chain of two commands in the `single_sim.py` script.
Note that the code in the `gromacs.py` module is assumed to be importable,
which it is in this case because it is part of the `scalems` package.

TODO: Explicit demonstration of chained simulations.
