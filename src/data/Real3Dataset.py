from src.data.Dataset import IDataset
import geopandas as gpd


class Real3Dataset(IDataset):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def createTables(self):
        # create the required postgres tables
        create_stmt = '''CREATE TABLE IF NOT EXISTS real3(
                    id SERIAL PRIMARY KEY NOT NULL,
                    geom GEOMETRY(Point, 26918) NOT NULL,
                    cid INT
                );'''
        self.pgc.execute(create_stmt)
        return

    def preprocess(self):
        # data generation and transformation if needed
        self.data = gpd.read_file(self.path)
        return

    def pushData(self):
        # insert the dataset into the postgres instance
        value_str = ", ".join([f'(ST_Point({point.x}, {point.y}))' for point in self.data['geometry']])
        insert_stmt = f'''INSERT INTO real3 (geom) VALUES {value_str};'''
        self.pgc.execute(insert_stmt)
        return

    def dropTables(self):
        # remove the previously created postgres tables
        drop_stmt = '''DROP TABLE real3;'''
        self.pgc.execute(drop_stmt)
