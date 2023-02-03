import threading
import time
import numpy as np
import pandas as pd
import os
from PostgresController import PostgresController
from proc.core import find_processes


class MemoryConsumptionMonitor():
    def __init__(self, interval):
        self.interval = interval
        self.memoryconsumptions = dict()
        self.pgc = PostgresController()

    def measurePG(self):
        STAT_QUERY = 'SELECT pid FROM pg_stat_activity;'
        pg_pids = self.pgc.executeFetch(STAT_QUERY)
        pg_pids = [pgs[0] for pgs in pg_pids]
        pg_procs = [pgp for pgp in find_processes() if pgp.pid in pg_pids]
        pg_memc = np.sum([p.rss for p in pg_procs])
        return pg_memc

    def measurePython(self):
        pyt_pid = os.getpid()
        pyt_procs = [pytp for pytp in find_processes() if pytp.pid == pyt_pid]
        if(len(pyt_procs) != 1):
            assert "Multiple python processes found"
        pyt_memc = pyt_procs[0].rss
        return pyt_memc

    def measure(self):
        memc = dict()
        memc["pg_memc"] = [self.measurePG()]
        memc["pyt_memc"] = [self.measurePython()]
        return memc

    def mergeMeasurements(self, d1, d2):
        if (len(d1.keys()) == 0):
            return d2
        for key in d1.keys():
            d1[key].extend(d2[key])
        return d1

    def monitor(self):
        while(not self.terminate):
            time.sleep(self.interval)
            memcons = self.measure()
            self.memoryconsumptions = self.mergeMeasurements(
                self.memoryconsumptions, memcons)

    def startMonitoring(self):
        self.terminate = False
        self.thread = threading.Thread(target=self.monitor)
        self.thread.start()

    def stopMonitoring(self):
        self.terminate = True
        self.thread.join()
        df_memc = pd.DataFrame(self.memoryconsumptions)
        return df_memc.max(axis=0)
