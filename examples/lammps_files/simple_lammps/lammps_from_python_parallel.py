# run as  mpirun -np 4 python lammps_from_python_parallel.py
from mpi4py import MPI
from lammps import lammps
lmp = lammps()
lmp.file("lammps_inputs/lj_bulk.input")
me = MPI.COMM_WORLD.Get_rank()
nprocs = MPI.COMM_WORLD.Get_size()
print("Proc %d out of %d procs has" % (me,nprocs),lmp)
MPI.Finalize()
