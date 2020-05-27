import sys
import logging
import gzip
import re
from datetime import datetime
import sqlite3
from contextlib import closing

import reader.line_reader
from reader.line_reader import FileLineReader, InvalidFormatError
import reader.processor.protocols as protocols
import database.database as database
from database.database import DatabaseWrtier



# Set format for logger
logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)
# Initialize logger
logger = logging.getLogger('Main')



class Listener(protocols.Listener):
    def __init__(self, db: DatabaseWrtier, lr: FileLineReader):
        self.db = db
        self.lr = lr

    def board_start(self, pair_name: str):
        # Create new table
        self.db.create_table_if_not_exists(pair_name, database.DEF_BOARD_TABLE)

    def board_insert(self, pair_name: str, data: dict):
        data['timestamp'] = self.lr.message_time()
        data['type'] = database.BoardRecordType.
        self.db.insert(data.pair_name, data)

    def board_clear(self, pair_name: str):
        # Complete board snapshot will delete all state in board
        inst = dict(
            timestamp=self.lr.message_time(),
            type=database.BoardRecordType.CLEAR_ALL,
            price=None,
            amount=None,
        )
        self.db.insert(pair_name, inst)

    def ticker_start(self, pair_name: str):
        self.db.create_table_if_not_exists(pair_name, database.DEF_TICKER_TABLE)

    def ticker_insert(self, pair_name: str, data: dict):
        # Insert data
        self.db.insert(pair_name, data)

    def eos(self):
        # Commit all to database
        self.db.commit()



if __name__ == '__main__':
    if len(sys.argv) <= 2:
        print('Please specify file to process, and a file name of datadase to write the result')
        exit(1)

    # Open compressed file with gzip with read, text option
    with gzip.open(sys.argv[1], 'rt') as file:
        with database.open(sys.argv[2]) as db:
            logger.info('Opening file...')
            reader = FileLineReader(file)
            reader.setup(Listener(db, reader))

            # Start reading
            logger.info('Processing lines from file...')
            try:
                while reader.next_line():
                    pass
            except EOFError as e:
                logger.exception('Reached EOF before explicit file terminal:\n%s' % e)
                exit(1)



