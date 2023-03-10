import os
from abc import ABC, abstractmethod
from enum import Enum
from PostgresController import PostgresController


class DatasetIndex(Enum):
    SYNTH1 = 1
    SYNTH2 = 2
    SYNTH3 = 3
    REAL1 = 4
    REAL2 = 5
    REAL3 = 6


class IDataset(ABC):
    def __init__(self):
        self.pgc = PostgresController()

    @staticmethod
    def getDataset(dataset_index, seed):
        from data.Synth1Dataset import Synth1Dataset
        from data.Synth2Dataset import Synth2Dataset
        from data.Synth3Dataset import Synth3Dataset
        from data.Real1Dataset import Real1Dataset
        from data.Real2Dataset import Real2Dataset
        from data.Real3Dataset import Real3Dataset
        match dataset_index:
            case DatasetIndex.SYNTH1:
                return Synth1Dataset(seed)
            case DatasetIndex.SYNTH2:
                return Synth2Dataset(seed)
            case DatasetIndex.SYNTH3:
                return Synth3Dataset(seed)
            case DatasetIndex.REAL1:
                return Real1Dataset(os.path.dirname(__file__) + '/real1/geo_export_5b9be610-442e-4118-8809-aea93e86134b.shp')
            case DatasetIndex.REAL2:
                return Real2Dataset()
            case DatasetIndex.REAL3:
                return Real3Dataset(os.path.dirname(__file__) + '/shp/ListedBuildings_10Jan2023.shp', 7405)
            case default:
                raise NotImplementedError()
                return

    @abstractmethod
    def createTables(self):
        # create the required postgres tables
        pass

    @abstractmethod
    def preprocess(self, n):
        # data generation and transformation if needed
        pass

    @abstractmethod
    def pushData(self):
        # insert the dataset into the postgres instance
        pass

    @abstractmethod
    def dropTables(self):
        # remove the previously created postgres tables
        pass

    def prepareDatabase(self, n=None):
        self.createTables()
        self.preprocess(n)
        self.pushData()
