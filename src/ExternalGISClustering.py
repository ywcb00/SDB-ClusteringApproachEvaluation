from Clustering import IClustering, ClusteringMethod
from data.Dataset import DatasetIndex
import os
from qgis.core import QgsApplication, QgsVectorLayer, QgsDataSourceUri, edit
from qgis.analysis import QgsNativeAlgorithms

QgsApplication.setPrefixPath(os.environ['QGIS_PREFIX_PATH'], True)
#QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
qgs = QgsApplication([], False)
qgs.initQgis()
import processing
from processing.core.Processing import Processing
Processing.initialize()


class ExternalGISClustering(IClustering):

    def __init__(self, res_dir):
        super().__init__(res_dir)

        self._data_uri = QgsDataSourceUri()
        self._data_uri.setConnection(
            os.environ['PG_HOST'], os.environ['PG_PORT'],
            os.environ['PG_DBNAME'], 
            os.environ['PG_USER'], os.environ['PG_PASSWD'])
        self._db_table_names = {
            DatasetIndex.SYNTH1: 'synth1', 
            DatasetIndex.SYNTH2: 'synth2', 
            DatasetIndex.SYNTH3: 'synth3', 
            DatasetIndex.REAL1: 'real1', 
            DatasetIndex.REAL2: 'real2', 
            DatasetIndex.REAL3: 'real3'
        }
        self._db_schema = os.environ['PG_DBSCHEMA']

        self._db_geom_col = 'geom'
        self._db_cid_col = 'cid'

    def preprocess(self, dataset_index):
        # preprocessing stuff like loading the data from postgres, etc.
        # return statistics
        self._data_uri.setDataSource(
            self._db_schema, 
            self._db_table_names[dataset_index], 
            self._db_geom_col)
        return

    def process(self, dataset_index, clustering_method):
        # the actual clustering task
        db_layer = QgsVectorLayer(self._data_uri.uri(), "db_layer", "postgres")
        assert(db_layer.isValid())
            
        param_dict = {
            "INPUT":db_layer, 
            "OUTPUT": 'TEMPORARY_OUTPUT'
        }

        match clustering_method:
            case ClusteringMethod.KMEANS:
                algorithm_id = "native:kmeansclustering"
                param_dict["CLUSTERS"] = 7
            case ClusteringMethod.DBSCAN:
                algorithm_id = "native:dbscanclustering"
            case default:
                raise NotImplementedError()

        res = processing.run(algorithm_id, param_dict)
        clust_layer = res['OUTPUT']

        cid_idx = db_layer.fields().indexFromName('cid')
        
        with edit(db_layer):
            for f_db, f_c in zip(db_layer.getFeatures(), clust_layer.getFeatures()):
                db_layer.changeAttributeValue(f_db.id(), cid_idx, f_c['CLUSTER_ID'])

        return 

    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        # return statistics
        # raise NotImplementedError()
        return
