"""
Example of a user-provided Analysis tool wrapper.

Wraps Pyemma to implement a Markov State Model builder and analyzer.
Set up a SCALE-MS compatible operation implementation and export it as the
module function `msm_analyzer`.
"""

import scalems

import pyemma.coor as coor
import pyemma.msm as msm

tol = 0.1


def relative_entropy(P, Q):
    """
    Takes two transition matrices, calculates relative entropy
    """
    # Implementation incomplete
    return rel_entropy_P_Q


class MSMAnalyzer:
    """
    Builds msm from output trajectory
    """

    def __init__(self, molecular_topology_file, trajectory, transition_matrix, num_clusters):
        # Build Markov model with PyEmma
        feat = coor.featurizer(molecular_topology_file)
        X = coor.load(trajectory, feat)
        Y = coor.tica(X, dim=2).get_output()
        k_means = coor.cluster_kmeans(Y, k=num_clusters)
        centroids = get_centroids(k_means)

        markov_model = msm.estimate_markov_model(kmeans.dtrajs, 100)  #

        previous_transition_matrix = transition_matrix
        self.transition_matrix = markov_model.get_transition_matrix()  # figure this out
        self._is_converged = relative_entropy(self.transition_matrix, transition_matrix) < tol

    def is_converged(self):
        return self._is_converged

    def get_transition_matrix(self):
        return self.transition_matrix


# Assuming MSMAnalyzer is an existing tool we do not want to modify,
# create a scalems compatible operation by wrapping with a provided utility.
msm_analyzer = scalems.make_operation(MSMAnalyzer,
                                      inputs=['topfile', 'trajectory', 'P', 'N'],
                                      output=['is_converged', 'transition_matrix']
                                      )
