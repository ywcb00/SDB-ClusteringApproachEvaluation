from Clustering import IClustering, ClusteringMethod
from data.Dataset import DatasetIndex, IDataset
from PostgresController import PostgresController
import geopandas as gpd
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans, DBSCAN


class ExternalMLClustering(IClustering):

    def __init__(self, res_dir):
        super().__init__(res_dir)
        self.pgc = PostgresController()
        self.table_names = ['synth1', 'synth2', 'synth3', 'real1', 'real2', 'real3']

    def preprocess(self, dataset_index):
        # preprocessing stuff like loading the data from postgres, etc.
        sql = f'SELECT * FROM {self.table_names[dataset_index.value-1]};'
        self.data = gpd.read_postgis(sql, self.pgc.engine)
        return

    def process(self, dataset_index, clustering_method):
        # the actual clustering task
        match dataset_index:
            case DatasetIndex.SYNTH1:
                table_name = 'synth1'
                k = 7
                eps = 1
                minpoints = 10
            case DatasetIndex.SYNTH2:
                table_name = 'synth2'
                k = 7
                eps = 0.001
                minpoints = 10
            case DatasetIndex.REAL1:
                table_name = 'real1'
                k = 7
                eps = 20
                minpoints = 10
            case DatasetIndex.REAL3:
                table_name = 'real3'
                k = 7
                eps = 20
                minpoints = 10
            case default:
                raise NotImplementedError()

        x_train = np.column_stack((
            pd.Series(self.data['geom'].apply(lambda p: p.x)),
            pd.Series(self.data['geom'].apply(lambda p: p.y))
        ))

        match clustering_method:
            case ClusteringMethod.KMEANS:
                # k-means clustering algorithm.
                kmeans = KMeans(k, n_init='auto')
                self.data['cid'] = kmeans.fit_predict(x_train)
            case ClusteringMethod.DBSCAN:
                # hierarchical clustering.
                dbscan = DBSCAN(eps=eps, min_samples=minpoints, metric='euclidean')
                self.data['cid'] = dbscan.fit_predict(x_train)
            case default:
                raise NotImplementedError()
        return

    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        table = f'{self.table_names[dataset_index.value-1]}'
        self.data.to_postgis(table, self.pgc.engine, if_exists='replace')
        return
