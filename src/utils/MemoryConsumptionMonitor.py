import threading
import time
import numpy as np
from PostgresController import PostgresController
from proc.core import find_processes


class MemoryConsumptionMonitor():
    def __init__(self, interval):
        self.interval = interval
        self.memoryconsumptions = np.empty(None)
        self.pgc = PostgresController()

    def measure(self):
        STAT_QUERY = 'SELECT pid FROM pg_stat_activity;'
        pg_pids = self.pgc.executeFetch(STAT_QUERY)
        pg_pids = [pgs[0] for pgs in pg_pids]
        pg_procs = [pgp for pgp in find_processes() if pgp.pid in pg_pids]
        pg_memc = np.sum([p.rss for p in pg_procs])
        return pg_memc

    def monitor(self):
        while(not self.terminate):
            time.sleep(self.interval)
            memcons = self.measure()
            self.memoryconsumptions = np.append(self.memoryconsumptions, memcons)

    def startMonitoring(self):
        self.terminate = False
        self.thread = threading.Thread(target=self.monitor)
        self.thread.start()

    def stopMonitoring(self):
        self.terminate = True
        self.thread.join()
        return np.max(self.memoryconsumptions)
