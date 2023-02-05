from data.Dataset import IDataset
import geoalchemy2 as ga2
import geopandas as gpd
import numpy as np
import sqlalchemy as sqla

class Real1Dataset(IDataset):
    def __init__(self, path):
        super().__init__()
        self.path = path
        self.table_name = 'real1'
        self.srid = 97305

    def createTables(self):
        # create the required postgres tables
        metadata = sqla.MetaData()
        real1_table = sqla.Table('real1', metadata,
            sqla.Column('id', sqla.Integer, primary_key=True),
            # temporary srid since geopandas is not aware of our srid 97305
            sqla.Column('geom', ga2.Geometry('POINT', srid=4326)),
            sqla.Column('cid', sqla.Integer)
        )
        real1_table.create(self.pgc.engine)
        return

    def preprocess(self, n=None):
        # data generation and transformation if needed
        self.data = gpd.read_file(self.path)
        self.data.rename({'geometry': 'geom'}, axis=1, inplace=True)
        self.data = self.data[-self.data['geom'].isna()] # drop missing values
        self.data['id'] = np.array(self.data.index)
        self.data.set_geometry('geom', inplace=True, crs="EPSG:4326")
        return

    def pushData(self):
        # insert the dataset into the postgres instance
        self.data[['id', 'geom']].to_postgis(self.table_name, self.pgc.engine, if_exists='append')
        # update the srid in the database
        # updatesrid_query = f'''SELECT UpdateGeometrySRID(\'{self.table_name}\', 'geom', {self.srid});'''
        # self.pgc.execute(updatesrid_query)
        return

    def dropTables(self):
        # remove the previously created postgres tables
        drop_stmt = f'''DROP TABLE IF EXISTS {self.table_name};'''
        self.pgc.execute(drop_stmt)
        return
