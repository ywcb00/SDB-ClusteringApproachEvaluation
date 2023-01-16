#!/usr/bin/env python3

from data.Dataset import DatasetIndex, IDataset
from data.Synth1Dataset import Synth1Dataset
from Clustering import ClusteringApproach, ClusteringMethod, IClustering
from IntegratedSDBClustering import IntegratedSDBClustering
from PostgresController import PostgresController

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

def performClustering(dataset_index, clustering_approach, clustering_method):
    print("---", "Clustering data", dataset_index.name, "with", clustering_approach.name, "---")
    clust = IClustering.getClusteringApproach(clustering_approach)
    clust.processAll(dataset_index, clustering_method)

def main():
    dataset_index = DatasetIndex.REAL3
    clustering_approach = ClusteringApproach.SDB
    clustering_method = ClusteringMethod.KMEANS
    loadDataToPostGIS(dataset_index)
    performClustering(dataset_index, clustering_approach, clustering_method)
    deleteDataFromPostGIS(dataset_index)
    return

if __name__ == '__main__':
    main()
