#!/usr/bin/env python3

from data.Dataset import DatasetIndex, IDataset
from data.Synth1Dataset import Synth1Dataset
from Clustering import ClusteringApproach, ClusteringMethod, IClustering
from IntegratedSDBClustering import IntegratedSDBClustering
import os
from PostgresController import PostgresController

MEASURE_PERFORMANCE = False
MEASURE_MEMCONS = False

eval_configs = [
    {'n': 200000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 400000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 600000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 800000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 1000000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.KMEANS},
    
    {'n': 50000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 100000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 150000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 200000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 250000, 'dataset_index': DatasetIndex.SYNTH1, 'clustering_method': ClusteringMethod.DBSCAN},
    
    {'n': 200000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 400000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 600000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 800000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': 1000000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.KMEANS},
    
    {'n': 25000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 50000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 75000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 100000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.DBSCAN},
    {'n': 125000, 'dataset_index': DatasetIndex.SYNTH2, 'clustering_method': ClusteringMethod.DBSCAN},
    
    {'n': None, 'dataset_index': DatasetIndex.REAL1, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': None, 'dataset_index': DatasetIndex.REAL1, 'clustering_method': ClusteringMethod.DBSCAN},
    
    {'n': None, 'dataset_index': DatasetIndex.REAL3, 'clustering_method': ClusteringMethod.KMEANS},
    {'n': None, 'dataset_index': DatasetIndex.REAL3, 'clustering_method': ClusteringMethod.DBSCAN},
]

def getResultsDirectory(dataset_index, clustering_approach, clustering_method, n):
    path = os.path.join("results", dataset_index.name,
        clustering_method.name, clustering_approach.name)
    if n is not None:
        path = os.path.join(path, str(n))
    return path

def loadDataToPostGIS(dataset_index, n):
    print("---", "Loading dataset", dataset_index.name, "---")
    data = IDataset.getDataset(dataset_index)
    data.prepareDatabase(n)
    return

def deleteDataFromPostGIS(dataset_index):
    print("---", "Deleting dataset", dataset_index.name, "---")
    data = IDataset.getDataset(dataset_index)
    data.dropTables()
    return

def performClustering(dataset_index, clustering_approach, clustering_method, res_dir, mp, mm):
    print("---", "Clustering data", dataset_index.name, "with", clustering_approach.name, "---")
    clust = IClustering.getClusteringApproach(clustering_approach, res_dir)
    clust.processAll(dataset_index, clustering_method, mp, mm)

def process(dataset_index, clustering_approach, clustering_method, n, mp, mm):
    res_dir = getResultsDirectory(dataset_index, clustering_approach, clustering_method, n)
    loadDataToPostGIS(dataset_index, n)
    performClustering(dataset_index, clustering_approach, clustering_method, res_dir, mp, mm)
    deleteDataFromPostGIS(dataset_index)

def main():
    eval_approaches = [ClusteringApproach.SDB, ClusteringApproach.GIS, ClusteringApproach.ML]
    repetitions = 1
    for measurePerf in [True, False]:
        mp = measurePerf
        mm = not measurePerf
        for counter in range(repetitions):
            print("Repetition", counter)
            for conf in eval_configs:
                print("Processing config", conf)
                for clustering_approach in eval_approaches:
                    dataset_index = conf['dataset_index']
                    clustering_method = conf['clustering_method']
                    n = conf['n']
                    process(dataset_index, clustering_approach, clustering_method, n, mp, mm)
    return

if __name__ == '__main__':
    main()
