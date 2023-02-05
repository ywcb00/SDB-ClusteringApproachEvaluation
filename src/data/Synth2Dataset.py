from data.Dataset import IDataset
from dataclasses import dataclass
import numpy as np

class Synth2Dataset(IDataset):
    @dataclass
    class ClusterConfig:
        mean: np.ndarray[(1,3), np.float64]
        cov: np.ndarray[(3,3), np.float64]
        prop: float # proprtion of the total size
        m: float

    def _make_cluster(self, config: ClusterConfig, size: int) -> np.array:
        clust_size = int(size*config.prop)
        cluster_coords = self._rng.multivariate_normal(config.mean, config.cov, clust_size)
        if self._discrete_m:
            return np.hstack([cluster_coords, np.vstack([config.m]*clust_size)])
        else:
            return np.hstack([cluster_coords, np.vstack(self._rng.normal(config.m, self._var_m, clust_size))])

    def _to_spd(self, a: np.ndarray[(1,9)]) -> np.ndarray[(3,3)]:
        a = a.reshape((3,3))
        return np.sqrt(np.dot(a, a.T))

    def _approx_transform_to_latlon(self, clusters: np.array) -> np.array:
        # we use the mean as center to base th latlon-coordinates on
        c_center = clusters[:, :2].mean(axis=0)
        distances = clusters[:, :2] - c_center
        distances_m = distances * self._offset_factor
        
        R = 6378137 # Earths radius
        dLat = distances_m[:, 0]/R 
        dLon = distances_m[:, 1]/(R*np.cos(np.pi*self._base_latlon[0]/180))
        dLatLon = np.vstack([dLat, dLon]).T
        latlon = self._base_latlon + dLatLon * 180/np.pi

        return np.hstack([latlon, clusters[:, 2:]])

        
    def __init__(self, seed):
        super().__init__()
        self._rng = np.random.Generator(np.random.MT19937(seed))
        self._discrete_m = False
        self._var_m = 0.3
        self._latlon_transform = True
        self._base_latlon = np.array([47.076747, 15.437288])
        self._offset_factor = 500 # offset for in meters for distance of 1
        self._cluster_configs = [
            self.ClusterConfig(np.array([6,6,6]), np.identity(3)*0.5, 30/100, 1),
            self.ClusterConfig(np.array([1,2,3]), np.identity(3)*1, 10/100, 2),
            self.ClusterConfig(np.array([10,0,0]), self._to_spd(self._rng.random(9)), 5/100, 3),
            self.ClusterConfig(np.array([0,6,9]), self._to_spd(self._rng.normal(2, 0.5, 9)), 25/100, 4),
            self.ClusterConfig(np.array([6,1,9]), np.identity(3)*np.array([1,2,3]), 30/100, 5)
        ]

    def createTables(self):
        create_stmt = 'CREATE TABLE IF NOT EXISTS synth2('\
            'id SERIAL PRIMARY KEY NOT NULL,'\
            'geom GEOMETRY(POINTZM, 4087),'\
            'cid INT);'

        self.pgc.execute(create_stmt)

    def preprocess(self, n=10000):
        assert(int(sum([c.prop for c in self._cluster_configs])) == 1)
        # data generation and transformation if needed
        self._data = np.concatenate([self._make_cluster(config, n) for config in self._cluster_configs])
        if self._latlon_transform:
            self._data = self._approx_transform_to_latlon(self._data)

    def pushData(self):
        # insert the dataset into the postgres instance
        # switch latlon to lonlat
        clusters = self._data
        clusters[:, [0,1]] = clusters[:, [1,0]]

        clusters[:, -1] = np.abs(clusters[:, -1]) # make the weight m strictly positive

        value_str = ", ".join([f'(ST_POINTZM({",".join(map(str, p))}))' for p in clusters])
        insert_stmt = f'INSERT INTO synth2 (geom) VALUES {value_str}'
        self.pgc.execute(insert_stmt)

    def dropTables(self):
        # remove the previously created postgres tables
        self.pgc.execute('DROP TABLE synth2;')
