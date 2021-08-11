import trajectory_continuer
from trajectory_continuer import TrajectoryContinuer

# Note that this input has to be placed by the user.
# References:
# * https://github.com/SCALE-MS/scale-ms/issues/75
# * https://github.com/SCALE-MS/scale-ms/issues/129
initial_structure=['lammps_inputs/lj_bulk.lmp']
all_frames = ["lammps_inputs/lj_bulk.dcd"]
max_iterations = 5

# initialize trajectory continuer by giving it an example coordinate file
tc = TrajectoryContinuer(max_iterations=max_iterations)

# extract the starting coordinates from the DCD file coordinates.
continue_structures = tc.generate_new_configurations(trajectory=all_frames)

# just print the coordinates to make sure they exist. 
for i, structure in enumerate(continue_structures):
    print(f"Structure {i}")
    try:
        print(structure['unitcell'])
        print(structure['positions'])
    except:
        print("can't extract structures from trajectories")

#test iteration limit
for i in range(max_iterations):
    tc.increment_iteration()
    if (tc.is_converged()):
        print(f"Iteration {i}: Yep, we're converged")
    else:
        print(f"Iteration {i}: Not converged yet.")
