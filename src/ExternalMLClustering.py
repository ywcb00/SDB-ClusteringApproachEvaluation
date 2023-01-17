from Clustering import IClustering, ClusteringMethod
from PostgresController import PostgresController
import geopandas as gpd
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans, AgglomerativeClustering


class ExternalMLClustering(IClustering):

    def __init__(self):
        super().__init__()
        self.pgc = PostgresController()
        self.table_names = ['synth1', 'synth2', 'synth3', 'real1', 'real2', 'real3']

    def preprocess(self, dataset_index):
        # preprocessing stuff like loading the data from postgres, etc.
        sql = f'SELECT * FROM {self.table_names[dataset_index.value-1]};'
        self.data = gpd.read_postgis(sql, self.pgc.engine)

        # return statistics
        # raise NotImplementedError()
        return

    def process(self, dataset_index, clustering_method):
        # the actual clustering task
        k = 7
        x_train = np.column_stack((
            pd.Series(self.data['geom'].apply(lambda p: p.x)),
            pd.Series(self.data['geom'].apply(lambda p: p.y))
        ))

        match clustering_method:
            case ClusteringMethod.KMEANS:
                # k-means clustering algorithm.
                kmeans = KMeans(k, n_init='auto')
                self.data['cid'] = kmeans.fit_predict(x_train)

            case ClusteringMethod.HIERARCHICAL:
                # hierarchical clustering.
                agc = AgglomerativeClustering(n_clusters=k, affinity='euclidean', linkage='ward')
                self.data['cid'] = agc.fit_predict(x_train)

            case default:
                raise NotImplementedError()

        # return statistics
        # raise NotImplementedError()
        return

    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        table = f'{self.table_names[dataset_index.value-1]}'
        self.data.to_postgis(table, self.pgc.engine, if_exists='replace')
        # return statistics
        # raise NotImplementedError()
        return
