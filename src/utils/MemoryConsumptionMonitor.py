import threading
import time
from datetime import datetime
import numpy as np
import pandas as pd
import os
from PostgresController import PostgresController
from proc.core import find_processes


class MemoryConsumptionMonitor():
    def __init__(self, interval, res_dir):
        self.interval = interval
        self.memoryconsumptions = dict()
        self.pgc = PostgresController()
        self.res_dir = res_dir

    def storeResults(self, df, prefix):
        if not os.path.isdir(self.res_dir):
            os.makedirs(self.res_dir)
        fname = f'memcons_{prefix}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
        fpath = os.path.join(self.res_dir, fname)
        with open(fpath, mode='w') as f:
            df.to_csv(f)

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

    def stopMonitoring(self, prefix):
        self.terminate = True
        self.thread.join()
        df_memc = pd.DataFrame(self.memoryconsumptions)
        df_max_memc = df_memc.max(axis=0)
        self.storeResults(df_max_memc, prefix)
        return df_max_memc
