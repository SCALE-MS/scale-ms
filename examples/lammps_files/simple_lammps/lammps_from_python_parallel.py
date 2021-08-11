# run as  mpirun -np 4 python lammps_from_python_parallel.py
import pathlib
import shutil

from lammps import lammps
from mpi4py import MPI

testdata = pathlib.Path(__file__).absolute().parent.parent.parent.parent / 'testdata'
inputs_src = testdata / 'lennardjones' / 'lammps'

working_dir = pathlib.Path('.').absolute()

me = MPI.COMM_WORLD.Get_rank()
if me == 0:
    shutil.copy(inputs_src / 'lj_bulk.input', working_dir)
    shutil.copy(inputs_src / 'lj_bulk.lmp', working_dir)
MPI.COMM_WORLD.barrier()

lmp = lammps()
lmp.file("lj_bulk.input")

nprocs = MPI.COMM_WORLD.Get_size()
print("Proc %d out of %d procs has" % (me,nprocs), lmp)
MPI.Finalize()
