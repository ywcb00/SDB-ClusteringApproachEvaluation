from data.Dataset import IDataset
import numpy as np

def getRandomNormalParams():
    mu_range = (-30, 30)
    var_range = (1, 30)
    mu = np.random.rand(2) * (abs(mu_range[1]-mu_range[0])) + mu_range[0]
    var = np.random.rand(2) * (abs(var_range[1]-var_range[0])) + var_range[0]
    cov = np.random.rand(1) * np.min(var)
    sigma = np.array([[var[0], cov[0]], [cov[0], var[1]]])
    return(mu, sigma)

def drawKNorm2D(n, k=10):
    norm_params = []
    for counter in range(k):
        mu, sigma = getRandomNormalParams()
        norm_params.append((mu, sigma))
    cluster_ratios = np.random.randint(low=1, high=n+1, size=k)
    cluster_ratios = np.rint(cluster_ratios * n / np.sum(cluster_ratios)).astype(int)
    cluster_ratios[-1] = cluster_ratios[-1] - (np.sum(cluster_ratios) - n)
    data = np.empty((0, 2))
    for counter in range(k):
        newdat = np.random.default_rng().multivariate_normal(
            mean=norm_params[counter][0], cov=norm_params[counter][1],
            size=cluster_ratios[counter])
        data = np.append(data, newdat, axis=0)
    np.random.shuffle(data)
    return data

class Synth1Dataset(IDataset):
    def __init__(self):
        super().__init__()

    def createTables(self):
        # create the required postgres tables
        create_stmt = '''CREATE TABLE IF NOT EXISTS synth1(
            id SERIAL PRIMARY KEY NOT NULL,
            geom GEOMETRY(Point, 26918) NOT NULL,
            cid INT
        );'''
        self.pgc.execute(create_stmt)
        return

    def preprocess(self, n=100000):
        # data generation and transformation if needed
        k = 10
        self.data = drawKNorm2D(n, k)
        return

    def pushData(self):
        # insert the dataset into the postgres instance
        value_str = ", ".join([f'(ST_Point({row[0]}, {row[1]}))' for row in self.data])
        insert_stmt = f'''INSERT INTO synth1 (geom) VALUES {value_str};'''
        self.pgc.execute(insert_stmt)
        return

    def dropTables(self):
        drop_stmt = '''DROP TABLE synth1;'''
        self.pgc.execute(drop_stmt)
