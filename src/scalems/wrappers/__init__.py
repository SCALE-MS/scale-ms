"""
Submodules for different simulation tools with normalized SCALE-MS friendly interface.

As we refine a normalized interface for simulation tools, we can provide
reference implementations as wrapper modules in the scalems.wrappers submodule.
Ultimately, we may be able to dispatch to the wrappers through a scalems.simulator
front end or something.

In the near term, wrappers provide a consistent interface to different simulation
tools. This allows better user experience and simplifies maintenance as we
refine the software design and implement new patterns for expressing simulation
work. It allows for some high level abstraction, anticipating future functionality
for interoperability between disparate tool sets.

All wrappers should have, to the extent possible, the same methods.

Core components:
    make_input(**kwargs) -> SimulationInput:
        A simulation tool module provides a *make_input* command that accepts
        implementation-dependent named arguments and returns an object that may
        be used as simulation input within the context of the other module tools.

    modify_input(simulation_input: SimulationInput, **kwargs) -> SimulationInput:
        A simulation input data source produced by *make_input* or *modify_input*
        may be modified in a pipe line through (implementation dependent) key word
        arguments to a *modify_input* command.

    simulate(simulation_input: SimulationInput, **kwargs) -> Simulation:
        Create a simulation operation instance. Accepts input from *make_input*
        or *modify_input*, along with key word arguments specified by the wrapper
        module.

Utility functions:
    internal_to_pdb(structure) -> scalems.FileFuture:
        For interoperability with other tools, simulator wrappers should provide
        conversion utilities for native structure and topology data. *internal_to_pdb*
        converts a native structure object to a structure file in the Protein Data Bank
        (PDB) format.

    get_trajectory(source) -> Trajectory:
        Get a Future for (currently implementation-specific) Trajectory data from
        a Simulation or other sensible source. Note: this free function allows us
        to defer the question of whether trajectory sources are assumed to have a
        `trajectory` instance attribute.

    collect_coordinates(trajectories: scalems.Iterable[scalems.Iterable[Conformation]]) -> scalems.Iterable[Conformation]:
        Some data hierarchies or topological transformations need to be performed
        by native tools. *collect_coordinates* creates a single iterable of
        system conformations from a collection of sources of conformation data.

Module types:
    Frame:
        Molecular system microstate data. Generally, a frame of simulation trajectory
        output, an input configuration, or a subset of simulation snapshot/checkpoint data.
        At a minimum, the member data is assumed to include atomic coordinates,
        indexed consistently with other Frames extracted from the same source.

    SimulationInput:
        Packages simulation inputs for consumption by other tools in the simulation
        package. If array-like or set-like input is provided, the SimulationInput
        has array-like or set-like behavior and implies ensemble simulation handling.
        Otherwise, the nature of the SimulationInput reference is a detail of the
        simulation tool wrapper.

    Simulation:
        Represents a simulation instance (or simulation ensemble). Named outputs
        are not yet well defined, but must include "trajectory" and something that
        is convertible to SimulationInput. The Simulation reference produced by
        a *simulate()* command has the data flow shape of the SimulationInput
        provided to it.

    Trajectory:
        A data Future representing molecular system trajectory data. For MD, this
        is presumed to be a trajectory output file or set of sequenced files.
        For a Simulation reference encompassing an ensemble or batch of simulations,
        outer dimensions of the Trajectory reference will describe the same shape
        as the source. A Trajectory has SCALE-MS sequence semantics. The wrapper
        should allow sequence-based data shaping commands to treat a Trajectory
        as a sequence of Frame data.

"""