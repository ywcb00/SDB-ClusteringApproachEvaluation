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
        self._db_layer = QgsVectorLayer(self._data_uri.uri(), "db_layer", "postgres")
        assert(self._db_layer.isValid())
            
        param_dict = {
            "INPUT":self._db_layer, 
            "OUTPUT": 'TEMPORARY_OUTPUT'
        }

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

        match clustering_method:
            case ClusteringMethod.KMEANS:
                algorithm_id = "native:kmeansclustering"
                param_dict["CLUSTERS"] = 7
            case ClusteringMethod.DBSCAN:
                algorithm_id = "native:dbscanclustering"
                param_dict["EPS"] = eps
                param_dict["MIN_SIZE"] = minpoints
            case default:
                raise NotImplementedError()

        res = processing.run(algorithm_id, param_dict)
        self._clust_layer = res['OUTPUT']

        return 

    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        cid_idx = self._db_layer.fields().indexFromName('cid')
        
        with edit(self._db_layer):
            id_cluster_dict =  {f_db.id(): f_c['CLUSTER_ID'] for f_db, f_c in zip(self._db_layer.getFeatures(), self._clust_layer.getFeatures())}
            self._db_layer.changeAttributeValues(cid_idx, id_cluster_dict)
        
        return
