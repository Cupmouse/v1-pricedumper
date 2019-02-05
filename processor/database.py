import sqlite3
import datetime
import time

def _adapt_datetime(dt: datetime.datetime):
    # [unix epch time] * 1000000 + microsecond
    return int(dt.timestamp()) * 1000000 + dt.microsecond

# For every input with datetime class, it will be converted using this function
# datetime -> int
sqlite3.register_adapter(datetime.datetime, _adapt_datetime)

class DatabaseWrtier(object):
    def __init__(self):
        self.connection = None

    def connect(self, url: str):
        if self.connection is not None and not self.connection.closed:
            raise RuntimeError('Database not closed')
        self.connection = sqlite3.connect(url)

    # tdef is table defnition
    def create_table_if_not_exists(self, table_name: str, tdef: dict):
        dt = ','.join(['`%s` %s' % (key, val) for key, val in tdef.items()])
        self.connection.execute('CREATE TABLE IF NOT EXISTS %s (%s)' % (table_name, dt))
        
    def insert(self, table_name: str, data: dict):
        self.connection.execute('INSERT INTO %s VALUES(%s)' % (table_name, ','.join(['?' for i in range(len(data))])), tuple(data.values()))
        self.connection.commit()

    def __del__(self):
        if self.connection is not None:
            self.connection.close()
