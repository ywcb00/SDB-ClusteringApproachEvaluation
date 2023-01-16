#!/usr/bin/env python3

from data.Dataset import DatasetIndex, IDataset
from data.Synth1Dataset import Synth1Dataset
from Clustering import ClusteringApproach, IClustering
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

def performClustering(dataset_index, clustering_approach):
    print("---", "Clustering data", dataset_index.name, "with", clustering_approach.name, "---")
    clust = IClustering.getClusteringApproach(clustering_approach)
    clust.processAll(dataset_index)

def main():
    pg_ctrl = PostgresController()
    con  = pg_ctrl.connect()
    print(con.closed)
    #dataset_index = DatasetIndex.SYNTH1
    #clustering_approach = ClusteringApproach.SDB
    #loadDataToPostGIS(dataset_index)
    #performClustering(dataset_index, clustering_approach)
    #deleteDataFromPostGIS(dataset_index)
    return

if __name__ == '__main__':
    main()
