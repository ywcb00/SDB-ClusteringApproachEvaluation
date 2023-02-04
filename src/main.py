#!/usr/bin/env python3

from data.Dataset import DatasetIndex, IDataset
from data.Synth1Dataset import Synth1Dataset
from Clustering import ClusteringApproach, ClusteringMethod, IClustering
from IntegratedSDBClustering import IntegratedSDBClustering
import os
from PostgresController import PostgresController

MEASURE_PERFORMANCE = False
MEASURE_MEMCONS = False

def getResultsDirectory(dataset_index, clustering_approach, clustering_method):
    path = os.path.join("results", dataset_index.name,
        clustering_method.name, clustering_approach.name)
    return path

def loadDataToPostGIS(dataset_index):
    print("---", "Loading dataset", dataset_index.name, "---")
    data = IDataset.getDataset(dataset_index)
    data.prepareDatabase()
    return

def deleteDataFromPostGIS(dataset_index):
    print("---", "Deleting dataset", dataset_index.name, "---")
    data = IDataset.getDataset(dataset_index)
    data.dropTables()
    return

def performClustering(dataset_index, clustering_approach, clustering_method, res_dir):
    print("---", "Clustering data", dataset_index.name, "with", clustering_approach.name, "---")
    clust = IClustering.getClusteringApproach(clustering_approach, res_dir)
    clust.processAll(dataset_index, clustering_method, MEASURE_PERFORMANCE, MEASURE_MEMCONS)

def process(dataset_index, clustering_approach, clustering_method):
    res_dir = getResultsDirectory(dataset_index, clustering_approach, clustering_method)
    loadDataToPostGIS(dataset_index)
    performClustering(dataset_index, clustering_approach, clustering_method, res_dir)
    deleteDataFromPostGIS(dataset_index)

def main():
    dataset_index = DatasetIndex.SYNTH2
    clustering_approach = ClusteringApproach.SDB
    clustering_method = ClusteringMethod.KMEANS
    process(dataset_index, clustering_approach, clustering_method)
    return

if __name__ == '__main__':
    main()
