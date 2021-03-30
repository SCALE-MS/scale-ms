"""
lammps simulation tools.
Preparation, manipulation, and output use command line tools.

"""
import scalems
import logging
import pathlib

from scalems.context import WorkflowManager
from scalems.exceptions import MissingImplementationError
from scalems.utility import next_monotonic_integer as _next_int

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))


import scalems

# Declare the public interface of this wrapper module.
__all__ = ['make_input', 'internal_to_xyz', 'collect_coordinates', 'simulate', 'modify_input']

def expand_input(infile, include_files=[], filetype=None): 
   '''
   parameters
   
   infile: string, the initial input file

   input_files: input files mentioned in infile

   '''

   all_lines = []
   try:
      if filetype == 'datafile':
         is_firstline = True
      with open(infile, "r") as ifile:
         for line in ifile:
            if line.startswith("XXXXread_data"):  # OK, we DON'T want this read for now
               vals = line.split()
               try:
                  if vals[1] in [pathlib.Path(name).parts[-1] for name in include_files]:
                     for incl in include_files:
                        if pathlib.Path(incl).parts[-1] == vals[1]:
                           more_lines = expand_input(incl,include_files.remove(incl),filetype='datafile')
                        all_lines.extend(more_lines)
               except:
                  BaseException(f"{vals[1]} in include command not given in list of include files")
            else:
               if filetype == 'datafile':
                  if is_firstline:
                     is_firstline = False
                  else:
                     all_lines.append(line)
               else:
                  all_lines.append(line)               
   except:
       BaseException(f"{infile} does not exist")
                       
   return all_lines

def make_input(simulation_parameters = 'input.in',
               included_inputs = [[]],
               # could be a list of files, so will include_files need to be a list of lists?
               # will need to be properly managed with list comprehension.
               wrapper_name = 'lammps', return_file=None):
               # The gromacs code had "wrapper_name". What is this supposed to do?
    '''
    If specified, commands are stored in 'return file' if not, as a list of commands
    '''
    
    all_commands = expand_input(simulation_parameters,included_inputs)
    # Right now, "all_commands" is literally just an array of lines to be put into a file, and read into lammps
    # Should it be a LIST of lists of lines, or how should the "list comprehension" of multiple inputs be set up?

    if return_file is not None:
       f = open(return_file,'w')
       s = ''
       f.write(s.join(all_commands))
       f.close()
       return return_file
    else:
       return all_commands

def modify_structure(structure,config):
   raise MissingImplementationError()
   #for s, c in zip(structure,config):

    
def modify_input(substitutions = {}, input_commands={}):

   # currently, I implement this just by looping over the commands and replacing each line
   # with the key:value pair from the dictionary.  I'm sure there is a better way to do this.
   # for example, it may replace all commands of a given type, and we just want to replace
   # some of the commands that start the same.
   for i,command in enumerate(input_commands):
      for k in substitutions:
         if command.starts_with(k):
            input_commands[i] = f"{k} {input_commands[k]}"
            
def simulate(lammps_binary, input_commands):

   '''
   lammps_binary = binary to use for lammps
   input_commands = a file containing the input commands
   '''
   
   #MRS: LAMMPS can also take commands from STDIN, so it might make more sense to pass them in from
   #STDIN.  However, it expects a series of lines from stdin, and scalems appears remove all the newlines.
   #s=''
   #simulation = scalems.executable(argv=[lammps_binary], stdin = s.join(input_commands), stdout='stdout')
   simulation = scalems.executable(argv=[lammps_binary], inputs={'-in':input_commands}, stdout='stdout')
   
   # output files are specified in the input file.  Should we parse the lammps file to
   # figure out these files, or just let lammps handle them
   
def internal_to_pdb(structure):
   # lammps doesn't have any files that handle processing cordinates.  That's done by other programs.
   # it can dump output with a larger number of output formats.  See:
   # https://lammps.sandia.gov/doc/dump.html
   return structure

def collect_coordinates(trajectories):
   # lammps doesn't have coordinate analysis routines, so right now, this is just a collector of trajectory names.
   allframes = scalems.gather(trajectories)
   return allframes

