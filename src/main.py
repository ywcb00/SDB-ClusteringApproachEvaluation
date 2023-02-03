#!/usr/bin/env python3

from data.Dataset import DatasetIndex, IDataset
from data.Synth1Dataset import Synth1Dataset
from Clustering import ClusteringApproach, ClusteringMethod, IClustering
from IntegratedSDBClustering import IntegratedSDBClustering
import os
from PostgresController import PostgresController
from utils.MemoryConsumptionMonitor import MemoryConsumptionMonitor

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

def performClustering(dataset_index, clustering_approach, clustering_method):
    print("---", "Clustering data", dataset_index.name, "with", clustering_approach.name, "---")
    clust = IClustering.getClusteringApproach(clustering_approach)
    clust.processAll(dataset_index, clustering_method)

def process(dataset_index, clustering_approach, clustering_method):
    res_dir = getResultsDirectory(dataset_index, clustering_approach, clustering_method)
    if MEASURE_MEMCONS:
        memconmon = MemoryConsumptionMonitor(1, res_dir)
        memconmon.startMonitoring()
    loadDataToPostGIS(dataset_index)
    performClustering(dataset_index, clustering_approach, clustering_method)
    deleteDataFromPostGIS(dataset_index)
    if MEASURE_MEMCONS:
        memcon = memconmon.stopMonitoring()
        print("Measured memory consumption:", memcon)

def main():
    dataset_index = DatasetIndex.REAL1
    clustering_approach = ClusteringApproach.SDB
    clustering_method = ClusteringMethod.KMEANS
    process(dataset_index, clustering_approach, clustering_method)
    return

if __name__ == '__main__':
    main()
