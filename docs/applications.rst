============
Applications
============

Adaptive ensemble simulation methods are implemented in terms of data flow
operations and control flow logic.

Examples follow.

Adaptive Markov State Modeling
==============================

In this example, we use a Python data flow scripting interface to connect
PyEmma, a simulator adapter, and some additional Python code to iteratively
run and analyze groups of simulations until convergence.

The main script body defines and executes the subgraph.
There is obvious opportunity for abstraction, but it may be illustrative in the
present case to mix implementation details with algorithmic structure,
in part to stimulate discussion and understanding of packaging and component
capabilities.

One supporting module (:file:`analyze.py`) wraps PyEmma.

Another supporting module (:file:`gromacsmd.py`) wraps GROMACS command line tools and gmxapi.

Main script body::

    from gmxapi import modify_input
    from gmxapi import mdrun as simulate

    from gromacsmd import collect_samples, gro_to_pdb, make_input

    # Could start with a list of distinct confs, but here we use a single starting point.
    # GROMACS MD model and method parameters.
    starting_structure = 'input_conf.gro' # GROMACS structure file.
    topology_file = 'input.top' # GROMACS topology input.
    run_parameters = 'params.mdp' # GROMACS simulation parameters.

    initial_simulation_input = make_input(
        simulation_parameters=run_parameters,
        topology=topology_file,
        conformation=starting_structure)

    # Set up an array of N simulations, starting from a single input.
    initial_input = scalems.broadcast(initial_simulation_input, shape=(N,))

    # We will need a pdb for MSM building in PyEmma
    initial_pdb = gro_to_pdb(starting_structure)

    # Get a placeholder object that can serve as a sub context / work graph owner
    # and can be used in a control operation.
    subgraph = scalems.subgraph(variables={
                                    'conformation': initial_input,
                                    'P': scalems.ndarray(0., shape=(N, N)),
                                    'is_converged': False})

    with subgraph:
        modified_input = modify_input(
            input=initial_input, structure=subgraph.conformation)
        md = simulate(input=modified_input)

        # Get the output trajectories and pass to PyEmma to build the MSM
        # Return a stop condition object that can be used in gmx while loop to
        # terminate the simulation
        allframes = collect_samples(md.output.trajectory)

        adaptive_msm = analysis.msm_analyzer(topfile=editconf.file['-o'],
            trajectory=allframes,
            P=subgraph.P)
        # Update the persistent data for the subgraph
        subgraph.P = adaptive_msm.output.transition_matrix
        # adaptive_msm here is responsible for maintaining the ensemble width
        subgraph.conformation = adaptive_msm.output.conformation
        subgraph.is_converged = adaptive_msm.output.is_converged

    # In the default work graph, add a node that depends on `condition` and
    # wraps subgraph.
    my_loop = gmx.while_loop(operation=subgraph,
                             condition=scalems.logical_not(subgraph.is_converged))
    my_loop.run()

The algorithmic details of the above scriplet depend on two supporting modules,
given here for completeness.

analyze.py::

    """
    Analysis tool for adaptive msms
    """

    import gmx
    import pyemma
    import pyemma.coor as coor
    import pyemma.msm as msm

    tol = 0.1


    def relative_entropy(P, Q):
        """
        Takes two transition matrices, calculates relative entropy
        """
        # Implementation incomplete
        return rel_entropy_P_Q


    class MSMAnalyzer:
        """
        Builds msm from gmxapi output trajectory
        """
        def __init__(self, topfile, trajectory, P, N):
            # Build markov model with PyEmma
            feat = coor.featurizer(topfile)
            X = coor.load(trajectory, feat)
            Y = coor.tica(X, dim=2).get_output()
            k_means = coor.cluster_kmeans(Y, k=N)
            centroids = get_centroids(k_means)

            M = msm.estimate_markov_model(kmeans.dtrajs, 100)

            # Q = n-1 transition matrix, P = n transition matrix
            Q = P
            self.P = M.get_transition_matrix()  # figure this out
            self._is_converged = relative_entropy(self.P, Q) < tol

        def is_converged(self):
            return self._is_converged

        def transition_matrix(self):
            return self.P

    # Assuming MSMAnalyzer is an existing tool we do not want to modify,
    # create a scalems compatible operation by wrapping with a provided utility.
    msm_analyzer = scalems.make_operation(MSMAnalyzer,
        inputs=['topfile', 'trajectory', 'P', 'N'],
        output=['is_converged', 'transition_matrix']
    )

Simulation preparation and output
manipulation use command line tools. Simulation is executed with gmxapi.

gromacsmd.py::

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
            input_files = {'-f': structure},
            output_files = {'-o': scalems.OutputFile(suffix='.pdb')})
        return editconf.output.files['-o']

    def collect_samples(trajectories):
        allframes = scalems.commandline_operation('gmx', 'trajcat',
            input_files={'-f': scalems.gather(trajectories)},
            output_files={'-o': scalems.OutputFile(suffix='.trr')})
        return allframes.output.file['-o']
