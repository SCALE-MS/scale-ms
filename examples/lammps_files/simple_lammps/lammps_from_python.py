import pathlib
import shutil

from lammps import lammps

testdata = pathlib.Path(__file__).absolute().parent.parent.parent.parent / 'testdata'
inputs_src = testdata / 'lennardjones' / 'lammps'

# Assumes we are in a suitable working directory.
working_dir = pathlib.Path('.').absolute()
shutil.copy(inputs_src / 'lj_bulk.input', working_dir)
shutil.copy(inputs_src / 'lj_bulk.lmp', working_dir)

lmp = lammps()
lmp.file("lj_bulk.input")
