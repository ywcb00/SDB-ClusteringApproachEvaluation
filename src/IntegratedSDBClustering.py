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
        # raise NotImplementedError()
        return {"time": 0, "memcons": 0}

    def process(self, dataset_index, clustering_method):
        # the actual clustering task
        # return statistics
        match dataset_index:
            case DatasetIndex.SYNTH1:
                self.processSynth1(clustering_method)
            case default:
                raise NotImplementedError()
        return

    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        # return statistics
        # raise NotImplementedError()
        return {"time": 0, "memcons": 0}

    def processSynth1(self, clustering_method):
        cluster_query = ''
        match clustering_method:
            case ClusteringMethod.KMEANS:
                k = 7
                cluster_query = f'''SELECT id,
                        ST_ClusterKMeans(geom, {k})
                            over ()
                        AS cid
                    FROM synth1'''
            case ClusteringMethod.DBSCAN:
                eps = 1
                minpoints = 10
                cluster_query = f'''SELECT id,
                        ST_ClusterDBSCAN(geom, eps := {eps}, minpoints := {minpoints})
                            over ()
                        AS cid
                    FROM synth1'''
                update_query = f'''UPDATE synth1
                    SET cid = sc.cid
                    FROM (SELECT id, ST_ClusterDBSCAN(geom, eps := {eps}, minpoints := {minpoints}) over () AS cid FROM synth1) AS sc
                    WHERE synth1.id = sc.id;
                '''
            case default:
                raise NotImplementedError()

        update_query = f'''UPDATE synth1
            SET cid = sc.cid
            FROM ({cluster_query}) AS sc
            WHERE synth1.id = sc.id;
        '''
        self.pgc.execute(update_query)
