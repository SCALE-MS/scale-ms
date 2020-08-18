"""Workflow contexts for MPI based multiprocessing.

Assuming target execution resources support MPI2 process spawning, we can manage
MPI initialization and finalization within the main interpreter process, rather
than relying on an invocation like `mpiexec -n ${N} python -m mpi4py ...`

When we can determine that there is no preexisting process group, we can
spawn a process group to provide execution agents.

Note: As with mpi4py.run, we would need to monitor the spawned processes for failures
so that we can issue MPI.COMM_WORLD.Abort() for the surviving spawned processes
to resolve deadlocks. See mpi4py.run.set_abort_status().

"""