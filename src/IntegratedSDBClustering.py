from Clustering import ClusteringMethod, IClustering
from data.Dataset import DatasetIndex
from PostgresController import PostgresController

class IntegratedSDBClustering(IClustering):
    def __init__(self):
        super().__init__()
        self.pgc = PostgresController()

    def preprocess(self, dataset_index):
        # preprocessing stuff like loading the data from postgres, etc.
        # return statistics
        return {"time": 0, "memcons": 0}

    def process(self, dataset_index, clustering_method):
        # the actual clustering task
        # return statistics
        match dataset_index:
            case DatasetIndex.SYNTH1:
                self.processGeomClustering(dataset_index, clustering_method)
            case DatasetIndex.REAL1:
                self.processGeomClustering(dataset_index, clustering_method)
            case default:
                raise NotImplementedError()
        return

    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        # return statistics
        return {"time": 0, "memcons": 0}

    def processGeomClustering(self, dataset_index, clustering_method):
        match dataset_index:
            case DatasetIndex.SYNTH1:
                table_name = 'synth1'
                k = 7
                eps = 1
                minpoints = 10
            case DatasetIndex.REAL1:
                table_name = 'real1'
                k = 5
                eps = 4
                minpoints = 30
            case default:
                raise NotImplementedError()

        match clustering_method:
            case ClusteringMethod.KMEANS:
                cluster_query = f'''SELECT id,
                        ST_ClusterKMeans(geom, {k})
                            over ()
                        AS cid
                    FROM {table_name}
                '''
            case ClusteringMethod.DBSCAN:
                cluster_query = f'''SELECT id,
                        ST_ClusterDBSCAN(geom, eps := {eps}, minpoints := {minpoints})
                            over ()
                        AS cid
                    FROM {table_name}
                '''
            case default:
                raise NotImplementedError()

        update_query = f'''UPDATE {table_name}
            SET cid = clustdat.cid
            FROM ({cluster_query}) AS clustdat
            WHERE {table_name}.id = clustdat.id;
        '''
        self.pgc.execute(update_query)
