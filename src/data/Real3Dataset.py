from data.Dataset import IDataset
import geopandas as gpd
import geoalchemy2 as ga2
import sqlalchemy as sqla
import numpy as np


class Real3Dataset(IDataset):
    def __init__(self, path, srid):
        super().__init__()
        self.path = path
        self.srid = srid

    def createTables(self):
        # create the required postgres tables
        metadata = sqla.MetaData()
        real3_table = sqla.Table('real3', metadata,
            sqla.Column('id', sqla.Integer, primary_key=True),
            sqla.Column('geom', ga2.Geometry('POINT', srid=self.srid)),
            sqla.Column('cid', sqla.Integer)
        )
        real3_table.create(self.pgc.engine)
        return

    def preprocess(self, n=None):
        # data generation and transformation if needed
        # adds cluster id field of type integer
        self.data = gpd.read_file(self.path)
        # self.data['cid'] = None
        # self.data['cid'] = self.data['cid'].astype('Int32')
        self.data.rename({'geometry': 'geom'}, axis=1, inplace=True)
        self.data = self.data[-self.data['geom'].isna()] # drop missing values
        self.data['id'] = np.array(self.data.index)
        self.data.set_geometry('geom', inplace=True, crs=f'EPSG:{self.srid}')
        return

    def pushData(self):
        # insert the dataset into the postgres instance
        self.data[['id', 'geom']].to_postgis('real3', self.pgc.engine, if_exists='append')
        return

    def dropTables(self):
        # remove the previously created postgres tables
        drop_stmt = '''DROP TABLE IF EXISTS real3;'''
        self.pgc.execute(drop_stmt)
