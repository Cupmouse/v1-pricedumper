import sqlite3
import datetime
import time
from enum import Enum

# Table definition for common data
DEF_TICKER_TABLE = dict(
    timestamp='INTEGER NOT NULL',
    best_bid='REAL NOT NULL',
    best_ask='REAL NOT NULL',
    best_bid_size='REAL NOT NULL',
    best_ask_size='REAL NOT NULL',
    total_bid_depth='REAL NOT NULL',
    total_ask_depth='REAL NOT NULL',
    last_traded_price='REAL NOT NULL',
    volume='REAL NOT NULL',
    volume_by_product='REAL NOT NULL',
)

DEF_BOARD_TABLE = dict(
    timestamp='INTEGER NOT NULL',
    type='INTEGER(3) NOT NULL',
    price='REAL',
    size='REAL'
)

class BoardRecordType(Enum):
    CLEAR_ALL = 0
    CLEAR_SELLS = 1
    CLEAR_BUYS = 2
    INSERT_SELL = 3
    INSERT_BUY = 4
    SET_SELL = 5
    SET_BUY = 6

def _adapt_board_record_type(type: BoardRecordType):
    return type.value

sqlite3.register_adapter(BoardRecordType, _adapt_board_record_type)

def _adapt_datetime(dt: datetime.datetime):
    # [unix epch time] * 1000000 + microsecond
    return int(dt.timestamp()) * 1000000 + dt.microsecond

# For every input with datetime class, it will be converted using this function
# datetime -> int
sqlite3.register_adapter(datetime.datetime, _adapt_datetime)

class DatabaseWrtier(object):
    def __init__(self):
        self._connection = None
        self._url = None

    def open(self, url: str):
        if self._connection is not None:
            raise RuntimeError('Database not closed')
        self._url = url
        self._connection = sqlite3.connect(url)

    def close(self):
        self._connection.close()

    # tdef is table defnition
    def create_table_if_not_exists(self, table_name: str, tdef: dict):
        dt = ','.join(['`%s` %s' % (key, val) for key, val in tdef.items()])
        self._connection.execute('CREATE TABLE IF NOT EXISTS %s (%s)' % (table_name, dt))
        
    def insert(self, table_name: str, data: dict):
        self._connection.execute('INSERT INTO %s VALUES(%s)' % (table_name, ','.join(['?' for i in range(len(data))])), tuple(data.values()))

    def commit(self):
        self._connection.commit()

    def __del__(self):
        if self._connection is not None:
            self._connection.close()

class open():
    def __init__(self, url: str):
        self._db = DatabaseWrtier()
        self._url = url

    def __enter__(self):
        self._db.open(self._url)
        return self._db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db.close()