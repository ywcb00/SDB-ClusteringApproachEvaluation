from data.Dataset import IDataset
import geopandas as gpd
import geoalchemy2
import numpy as np


class Real3Dataset(IDataset):
    def __init__(self, path, crs):
        super().__init__()
        self.path = path
        self.data = gpd.read_file(self.path)
        self.data.crs = crs

    def createTables(self):
        # create the required postgres tables
        return

    def preprocess(self, n=None):
        # data generation and transformation if needed
        # adds cluster id field of type integer
        self.data['cid'] = None
        self.data['cid'] = self.data['cid'].astype('Int32')
        self.data.rename({'geometry': 'geom'}, axis=1, inplace=True)
        self.data.set_geometry('geom', inplace=True)
        return

    def pushData(self):
        # insert the dataset into the postgres instance
        self.data[['geom', 'cid']].to_postgis('real3', self.pgc.engine, if_exists='replace')
        return

    def dropTables(self):
        # remove the previously created postgres tables
        drop_stmt = '''DROP TABLE IF EXISTS real3;'''
        self.pgc.execute(drop_stmt)
