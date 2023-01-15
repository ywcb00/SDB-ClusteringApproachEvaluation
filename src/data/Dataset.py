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
    def getDataset(dataset_index):
        from data.Synth1Dataset import Synth1Dataset
        from data.Synth2Dataset import Synth2Dataset
        from data.Synth3Dataset import Synth3Dataset
        from data.Real1Dataset import Real1Dataset
        from data.Real2Dataset import Real2Dataset
        from data.Real3Dataset import Real3Dataset
        match dataset_index:
            case DatasetIndex.SYNTH1:
                return Synth1Dataset()
            case DatasetIndex.SYNTH2:
                return Synth2Dataset()
            case DatasetIndex.SYNTH3:
                return Synth3Dataset()
            case DatasetIndex.REAL1:
                return Real1Dataset()
            case DatasetIndex.REAL2:
                return Real2Dataset()
            case DatasetIndex.REAL3:
                return Real3Dataset()
            case default:
                raise NotImplementedError()
                return

    @abstractmethod
    def createTables(self):
        # create the required postgres tables
        pass

    @abstractmethod
    def preprocess(self):
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

    def prepareDatabase(self):
        self.createTables()
        self.preprocess()
        self.pushData()
