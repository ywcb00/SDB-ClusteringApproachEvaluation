from abc import ABC, abstractmethod
from enum import Enum

class ClusteringApproach(Enum):
    SDB = 1
    GIS = 2
    ML = 3

class IClustering(ABC):

    @staticmethod
    def getClusteringApproach(clustering_approach):
        from IntegratedSDBClustering import IntegratedSDBClustering
        from ExternalGISClustering import ExternalGISClustering
        from ExternalMLClustering import ExternalMLClustering
        match clustering_approach:
            case ClusteringApproach.SDB:
                return IntegratedSDBClustering()
            case ClusteringApproach.GIS:
                return ExternalGISClustering()
            case ClusteringApproach.ML:
                return ExternalMLClustering()
            case default:
                raise NotImplementedError()
                return

    @abstractmethod
    def preprocess(self, dataset_index):
        # preprocessing stuff like loading the data from postgres, etc.
        # return statistics
        pass

    @abstractmethod
    def process(self, dataset_index):
        # the actual clustering task
        # return statistics
        pass

    @abstractmethod
    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        # return statistics
        pass

    def processAll(self, dataset_index):
        print("---", "Preprocessing", "---")
        pre = self.preprocess(dataset_index)
        print("---", "Processing", "---")
        pro = self.process(dataset_index)
        print("---", "Postprocessing", "---")
        post = self.postprocess(dataset_index)
        return (pre, pro, post)
