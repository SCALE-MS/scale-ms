"""
Example of a user-provided trivial analysis tool.

Set up a SCALE-MS compatible operation implementation and export it as the
module function `trajectory_continuer`.
"""

#import scalems
import pathlib
from pathlib import Path
import MDAnalysis
from MDAnalysis.lib.formats.libdcd import DCDFile

class TrajectoryContinuer:
    """
    Continues a trajectory.  Could do this with restart files, but we want to
    have more flexibility for other types of analysis.
    """

    def __init__(self, max_iterations=10):
        
        self.iterations = 0
        self.max_iterations = max_iterations
        # this should be in make_input
        #self._save_start_data(input_config)


    #This below should be in make_input     
    '''    
    def _save_start_data(self,input_config):

        f = open(input_config,"r")
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
    '''
    
    def is_converged(self):
        return self.iterations >= self.max_iterations

    # this should probably be in modify_input; it's the code tht takes coordinates, and makes new files.
    '''
    def _insert_new_configurations(positions,unitcells):
        newfiles = list()
        for position, box in zip(positions,unitcells):
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
    '''

    # not clear if this should operate on a single trajectory, or a set of trajectories. 
    # Or both? Eric, guidance?
    def _get_last_configurations(self, trajectory):
        if len(trajectory) > 0:
            trajectories = trajectory # it's a list of files
        else:
            trajectories = [trajectory]

        end_configs = list()
        end_config = dict()
        for trj in trajectories:
            try:
                Path(trj).suffix == '.dcd'
            except:
                Exception("Can't currently handle LAMMPS file types other than dcd")
            try:
                with DCDFile(trj) as dcd:
                    n_frames = dcd.n_frames
                    dcd.seek(n_frames-1)  # because of numbering, we want one less.
                    dcdframe = dcd.read()
                    end_config['unitcell'] = dcdframe.unitcell
                    end_config['positions'] = dcdframe.xyz
            except:
                Exception("Can't extract configurations from the trajectory file")
            end_configs.append(end_config)    

        return end_configs

    def generate_new_configurations(self,trajectory):
        #  takes a trajectory, and generates a coordinates of the last frame of the trajectory
        
        # identify the last configurations, in terms of an array of 3D variables and a box vector.
        end_configs = self._get_last_configurations(trajectory)

        return end_configs
    
        # This below should be in modify_input
        # where one takes the coordinates, and writes new files.
        '''
        new_inputs = self._insert_new_configurations(end_config)
        outfiles = list()
         create the files
        for i, new_input in enumerate(new_inputs):
            fname = f'temp.{i}.lmp'
            f = open(fname,'w')
            f.write(new_input)
            f.close()
            outfiles.append(fname)
        return outfiles
        '''
        
    def increment_iteration(self):
        self.iterations += 1

# commenting out for now.

# create a scalems compatible operation by wrapping with a provided utility.
#trajectory_continuer = scalems.make_operation(TrajectoryContinuer,
#                                              inputs=['trajectory'],
#                                              output=['is_converged']
#                                              )

