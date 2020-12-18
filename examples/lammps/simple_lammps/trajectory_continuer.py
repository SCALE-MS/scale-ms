"""
Example of a user-provided trivial analysis tool.

Set up a SCALE-MS compatible operation implementation and export it as the
module function `trajectory_continuer`.
"""

import scalems
import pathlib
from pathlib import Path
import MDAnalysis
from MDAnalysis.lib.formats.libdcd import DCDFile
import pdb

class TrajectoryContinuer:
    """
    Continues a trajectory.  Could do this with restart files, but we want to
    have more flexibility for other types of analysis.
    """

    def __init__(self, input_config, max_iterations=10):
        
        self.iterations = 0
        self.max_iterations = max_iterations
        self._save_start_data(input_config)
        
    def _save_start_data(self,input_config):

        f = open(input_config,"w")
        # we basically save the whole file, except what is in the atoms
        self.initial_input_file = dict()
        lines = f.readlines()
        self.initial_input_file['input_file_lines'] = lines
        # number of atoms is the first integer on the first line
        self.inital_input_file['n_atoms'] = int(lines[2].split()[0])  
        
        for i, line in enumerate(lines):
            if line[0:5] == 'Atoms':
                self.initial_input_file['atoms_start'] = i+2
            vals = line.split()
            if vals[3] == 'xlo' and vals[4] == 'xhi':
                self.initial_input_file['box_start'] =  i
            
    def is_converged(self):
        return self.iterations < self.max_iterations

    def _insert_new_configurations(positions,boxes):
        newfiles = list()
        for position, box in zip(positions,boxes):
            input_file = self.initial_input_file
            natoms = input_file['n_atoms']
            lines = input_file['lines']
            newfile = list()
            for i, line in enumerate(lines):
                if i == input_file['box start']:
                    newfile.append(f'{box[0][0]:11.7f} {box[0][1]:11.7f} {box[0][2]:11.7f} xlow xhi')
                    newfile.append(f'{box[1][0]:11.7f} {box[1][1]:11.7f} {box[1][2]:11.7f} ylow yhi')
                    newfile.append(f'{box[2][0]:11.7f} {box[2][1]:11.7f} {box[2][2]:11.7f} zlow zhi')
                    i += 3
                if i == input_file['atoms_start']:
                    for n in self.initial_input_file['n_atoms']: 
                        newfile.append(f'{lines[i][0:32]}{positions[i][0]:13.7f}{positions[i][1]:13.7f}{positions[i][2]:13.7f}')
                        i+=1
                else:
                    newfile.append(lines[i])
            newfiles.append(newfile)
        return(newfiles)
            
    def _get_last_configuration(self, trajectory):
        if len(trajectory) > 0:
            trajectories = trajectory # it's a list of files
        else:
            trajectories = [trajectory]

        end_configs = dict()
        for trj in trajectories:
            try:
                Path(trj).suffix == '.dcd'
                with DCDFile(trajectory) as dcd:
                    dcd.seek(-1)
                    dcdframe = dcd.read()
                    end_configs['positions'] = dcframe.positions()
                    end_configs['unitcells'] = dcframe.box()
                # some
            except:
                Exception("Can't handle LAMMPS file types other than dcd")
        return end_configs

    def generate_new_configurations(self,trajectory):

        # identify the last configurations
        end_config = self._get_last_configurations(trajectory)

        # generate new inputs
        new_inputs = self._insert_new_configurations(end_config)

        outfiles = list()
        # create the files
        for i, new_input in enumerate(new_inputs):
            fname = f'temp.{i}.lmp'
            f = open(fname,'w')
            f.write(new_input)
            f.close()
            outfiles.append(fname)
        return outfiles
    
    def increment_iteration(self):
        self.iterations += 1

# commenting out for now.

# create a scalems compatible operation by wrapping with a provided utility.
#trajectory_continuer = scalems.make_operation(TrajectoryContinuer,
#                                              inputs=['trajectory'],
#                                              output=['is_converged']
#                                              )

