from data.Dataset import IDataset

class Real3Dataset(IDataset):
    def __init__(self):
        super().__init__()

    def createTables(self):
        # create the required postgres tables
        raise NotImplementedError()
        return

    def preprocess(self):
        # data generation and transformation if needed
        raise NotImplementedError()
        return

    def pushData(self):
        # insert the dataset into the postgres instance
        raise NotImplementedError()
        return

    def dropTables(self):
        # remove the previously created postgres tables
        raise NotImplementedError()
        return
