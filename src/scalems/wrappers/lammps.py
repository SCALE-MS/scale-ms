"""
lammps simulation tools.

Preparation, manipulation, and output use command line tools.

"""

import scalems

# Declare the public interface of this wrapper module.
__all__ = ['make_input', 'internal_to_xyz', 'collect_coordinates', 'simulate', 'modify_input']

def expand_input(infile,include_files=[]): 
   '''
   parameters
   
   infile: string, the initial input file

   input_files: input files mentioned in infile

   '''

   all_lines = []
   try:
       with open(infile, "r") as ifile:
           lines = f.readlines()
           for line in ifile:
               if line.startswith("input"):
                   vals = line.split()
                   try:
                       for vals[1] in input_files:
                          more_lines = expand_input(vals[1],input_files.remove(vals[1]))
                          all_lines.extend(more_lines)
                   except:
                       BaseException(f"{vals[1]} in include command not given in list of include files")
               else:    
                   all_lines.append(line)
   except:
       BaseException(f"{infile} does not exist")
                       
   return all_lines

    

def make_input(simulation_parameters = ['input.in'],
               included_files = [[]],
               # could be a list of files, so will include_files need to be a list of lists?
               # will need to be properly managed with list comprehension.
               wrapper_name = 'lammps'):
               # The gromacs code had "wrapper_name". What is this supposed to do?
    
    all_commands = expand_input(simulation_parameters,include_files)
    # Right now, "all_commands" is literally just an array of lines to be put into a file, and read into lammps
    # Should it be a LIST of lists of lines, or how should the "list comprehension" of multiple inputs be set up?

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

def simulate(input_commands,lammps_binary):

    # probably here want to take the array of commands and covert it into an input file.

    #MRS: LAMMPS can also take commands from STDIN, so it might make more sense to pass them in from
    #STDIN. 

    with open(input_file, "w") as outfile:
        outfile.write("\n".join(itemlist))
    
    simulation = scalems.executable(lammps_binary,inputs={'-in':input_file})
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

