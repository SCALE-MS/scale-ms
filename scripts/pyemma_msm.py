"""
Analysis tool for adaptive msms
"""


import pyemma
import pyemma.coor as coor
import pyemma.msm as msm

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

