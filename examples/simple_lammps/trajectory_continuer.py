"""
Example of a user-provided triviak analysis tool.

Set up a SCALE-MS compatible operation implementation and export it as the
module function `trajectory_continuer`.
"""

import scalems


class TrajectoryContinuer:
    """
    Continues a trajectory.
    """

    def __init__(self, trajectory, max_iterations=10):
        
        self.transition_matrix = markov_model.get_transition_matrix()  # figure this out
        self._is_converged = iterations >= max_iterations

    def is_converged(self):
        return self.iterations

    def get_transition_matrix(self):
        return self.transition_matrix


# Assuming MSMAnalyzer is an existing tool we do not want to modify,
# create a scalems compatible operation by wrapping with a provided utility.
trajectory_continuer = scalems.make_operation(TrajectoryContinuer,
                                              inputs=['trajectory'],
                                              output=['is_converged']
)
