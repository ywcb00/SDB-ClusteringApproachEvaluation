from Clustering import IClustering

class ExternalMLClustering(IClustering):

    def preprocess(self, dataset_index):
        # preprocessing stuff like loading the data from postgres, etc.
        # return statistics
        raise NotImplementedError()
        return

    def process(self, dataset_index):
        # the actual clustering task
        # return statistics
        raise NotImplementedError()
        return

    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        # return statistics
        raise NotImplementedError()
        return
