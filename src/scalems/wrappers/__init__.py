"""Submodules for different simulation tools with normalized SCALE-MS friendly interface.

As we refine a normalized interface for simulation tools, we can provide
reference implementations as wrapper modules in the scalems.wrappers submodule.
Ultimately, we may be able to dispatch to the wrappers through a scalems.simulator
front end or something.

In the near term, wrappers provide a _consistent_ interface to different simulation
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

        Proposal: At an abstract level, this contains everything needed to define the simulation
        mathematically. The chemical system, instructions for running the simulation, but
        not how it's executed, or the resources it will take. That will be taken care of later in
        the workflow.

        GROMACS: .mpd,.top,.gro
        OpenMM: (look at ExTasy workflows)
        PLUMED-GROMACS: GROMACS + PLUMED input file
        LAMMPS: .input file

        Crucially, it provide a place for the information to be staged
        for modifying input, such that a single simulation can be
        loaded, and then variants of that simulation can be "cloned out", and modified.

        For many programs, this may simply be putting the text of the
        files into a container where they can be retrived and
        modified.

    modify_input(simulation_input: SimulationInput, **kwargs) -> SimulationInput:

        A simulation input data source produced by *make_input* or *modify_input*
        may be modified in a pipe line through (implementation dependent) key word
        arguments to a *modify_input* command.

        For most options, then a keyword argument can directly
        parallel the keyword syntax used by the simulation programs
        themselves: LAMMPS, PLUMED, GROMACS (in the .mdp) all use
        keyword options.  It's not as clear for OpenMM, but we should
        use the ExTASY implementations to guide us as to how modification is done there.

        One question is how the defintion of the chemical system
        should changed using this approach. This is not set up very
        well in the keyword format.  We could, for now, just replace
        the file containing the molecular system definition entirely
        using a modify_input.  This might be the best bet for now, but
        we will want to think about what sorts of changes might be of
        interest, if any in the future.

        The issue is that in most cases, if the chemical system is
        changing, it usually requires changes of many lines of
        topologies, and so passing it in as a dictionary will likely
        be a big pain. Likely easier to take care of it outside the
        simulation for now. Eventually, if some parallel work on
        simulation topology definitions in Open Force Field works,
        then it may be possible to do this better.

        Things like "training potential" that get attached in the
        examples should probably be attached at this point in the
        workflow, rather than at the simulate. For most workflows, it
        could ONLY be added at this stage, since potentials are
        defined in the input files.  For GMX_API driven simulations,
        the actual attachment of the potentials to the md simulation
        could happen in the simulate function, but it would be better
        if they were loaded into the data structure in the
        modify_input sage.

    simulate(simulation_input: SimulationInput, **kwargs) -> Simulation:
        Create a simulation operation instance. Accepts input from *make_input*
        or *modify_input*, along with key word arguments specified by the wrapper
        module.

        Potentially, we use this to specify which resources assoicated
        with the job?  The goal of this function is to define a task
        that is ready to dispatch for execution.

        MRS: some notes that I took during the conversation last week:
        Any of the things that affect the checkpointable identity are
        immutable after simulate.  So after simulate, define an
        equivalence class. Operate by fingerprinting commands and data
        to determine identity that should distinguish it.  Workflow
        level checkpointing.

        MRS: I'm still a little confused by the point of this.  I
        assume the importance of checkpointing is to know whether one
        should relaunch a simulation if it fails?  And if it's not
        checkpointable?  Can we clarify this a bit more.


Utility functions:
    internal_to_pdb(structure) -> scalems.FileFuture:
        For interoperability with other tools, simulator wrappers should provide
        conversion utilities for native structure and topology data. *internal_to_pdb*
        converts a native structure object to a structure file in the Protein Data Bank
        (PDB) format.

        MRS: Though actually, PDB might not be the best, since it's limited resolution.
        Should it actually convert the internal representation into a numpy array (or something
        like mdtraj or mdanalysis internal representation, which can THEN be converted into
        PDB files or any other output trajectory form desired.

    get_trajectory(source) -> Trajectory:
        Get a Future for (currently implementation-specific) Trajectory data from
        a Simulation or other sensible source. Note: this free function allows us
        to defer the question of whether trajectory sources are assumed to have a
        `trajectory` instance attribute.

    collect_coordinates(trajectories: scalems.Iterable[scalems.Iterable[Conformation]]) ->
            scalems.Iterable[Conformation]:
        Some data hierarchies or topological transformations need to be performed
        by native tools. *collect_coordinates* creates a single iterable of
        system conformations from a collection of sources of conformation data.

        MRS: Then internal_to_pdb can be used to convert into external files as needed?


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
