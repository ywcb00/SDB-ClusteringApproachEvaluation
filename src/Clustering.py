from abc import ABC, abstractmethod
import csv
from datetime import datetime
from enum import Enum
import os
import time
from utils.MemoryConsumptionMonitor import MemoryConsumptionMonitor

class ClusteringApproach(Enum):
    SDB = 1
    GIS = 2
    ML = 3

class ClusteringMethod(Enum):
    KMEANS = 1
    DBSCAN = 2
    HIERARCHICAL = 3

class IClustering(ABC):

    def __init__(self, res_dir):
        self.res_dir = res_dir

    @staticmethod
    def getClusteringApproach(clustering_approach, res_dir):
        from IntegratedSDBClustering import IntegratedSDBClustering
        from ExternalGISClustering import ExternalGISClustering
        from ExternalMLClustering import ExternalMLClustering
        match clustering_approach:
            case ClusteringApproach.SDB:
                return IntegratedSDBClustering(res_dir)
            case ClusteringApproach.GIS:
                return ExternalGISClustering(res_dir)
            case ClusteringApproach.ML:
                return ExternalMLClustering(res_dir)
            case default:
                raise NotImplementedError()
                return

    def storeResults(self, val_dict):
        if not os.path.isdir(self.res_dir):
            os.makedirs(self.res_dir)
        fname = f'perf_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
        fpath = os.path.join(self.res_dir, fname)
        with open(fpath, mode='w') as f:
            writer = csv.DictWriter(f, fieldnames=list(val_dict.keys()))
            writer.writeheader()
            writer.writerows([val_dict])

    @abstractmethod
    def preprocess(self, dataset_index):
        # preprocessing stuff like loading the data from postgres, etc.
        # return statistics
        pass

    @abstractmethod
    def process(self, dataset_index, clustering_method):
        # the actual clustering task
        # return statistics
        pass

    @abstractmethod
    def postprocess(self, dataset_index):
        # postprocessing stuff like uploading the clustered data to postgres
        # return statistics
        pass

    def processAll(self, dataset_index, clustering_method,
        MEASURE_PERFORMANCE, MEASURE_MEMCONS):
        time_measurements = dict()
        memcon_delay = 1

        if MEASURE_PERFORMANCE:
            pre_t0 = time.perf_counter()
        if MEASURE_MEMCONS:
            memconmon = MemoryConsumptionMonitor(memcon_delay, self.res_dir)
            memconmon.startMonitoring()

        self.preprocess(dataset_index)

        if MEASURE_PERFORMANCE:
            pre_t1 = time.perf_counter()
            time_measurements["pre_time"] = (pre_t1 - pre_t0) * 10**(3) # milliseconds
            main_t0 = time.perf_counter()
        if MEASURE_MEMCONS:
            memcon = memconmon.stopMonitoring("pre")
            memconmon = MemoryConsumptionMonitor(memcon_delay, self.res_dir)
            memconmon.startMonitoring()

        self.process(dataset_index, clustering_method)

        if MEASURE_PERFORMANCE:
            main_t1 = time.perf_counter()
            time_measurements["main_time"] = (main_t1 - main_t0) * 10**(3) # milliseconds
            post_t0 = time.perf_counter()
        if MEASURE_MEMCONS:
            memcon = memconmon.stopMonitoring("main")
            memconmon = MemoryConsumptionMonitor(memcon_delay, self.res_dir)
            memconmon.startMonitoring()

        self.postprocess(dataset_index)

        if MEASURE_PERFORMANCE:
            post_t1 = time.perf_counter()
            time_measurements["post_time"] = (post_t1 - post_t0) * 10**(3) # milliseconds
            self.storeResults(time_measurements)
        if MEASURE_MEMCONS:
            memcon = memconmon.stopMonitoring("post")

        return
