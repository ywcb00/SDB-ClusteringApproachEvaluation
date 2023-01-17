import psycopg2
import os
from sqlalchemy import create_engine

class PostgresController:
    def __init__(self):
        self.dbname = os.environ['PG_DBNAME']
        self.host = os.environ['PG_HOST']
        self.port = os.environ['PG_PORT']
        self.user = os.environ['PG_USER']
        self.passwd = os.environ['PG_PASSWD']

    def connect(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.dbname,
            user=self.user,
            password=self.passwd
        )
        return conn

    def execute(self, sql_query):
        conn = self.connect()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()
        conn.close()

    def executeFetch(self, sql_query):
        conn=self.connect()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql_query)
        res = cursor.fetchall()
        conn.commit()
        conn.close()
        return res

    def getEngine(self):
        return create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(self.user,
                                                                       self.passwd,
                                                                       self.host,
                                                                       self.port,
                                                                       self.dbname))
