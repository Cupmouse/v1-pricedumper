import sqlite3
import datetime

conn = sqlite3.connect('test.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

cur = conn.cursor()

# cur.execute('CREATE TABLE ttt (time timestamp)')

cur.execute('INSERT INTO ttt VALUES(?)', (datetime.datetime.now(), ))

cur.execute('SELECT * FROM ttt')

conn.commit()

res = cur.fetchone()

print(res)

conn.close()

# class Board:
#     def __init__(self):
#         self.clear_sell()
#         self.clear_buy()
    
#     def clear_sell(self):
#         self._sells = []

#     def clear_buy(self):
#         self._buy = []

#     def set_sell_at(self, price, amount):
#         self._sells[price] = amount

#     def set_buy_at(self, price, amount):
#         self._buy[price] = amount

#     def insert_sell_at(self, price, amount):
#         self._sells[price] += amount

#     def insert_buy_at(self, price, amount):
#         self._buy[price] += amount