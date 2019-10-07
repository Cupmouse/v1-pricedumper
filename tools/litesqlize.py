import sys
import logging
import gzip
import re
from datetime import datetime

from reader.line_reader import FileLineReader, InvalidFormatError
import reader.processor.protocols as protocols
import database.database as database
from database.database import DatabaseWrtier

if __name__ == '__main__':
    if len(sys.argv) <= 2:
        print('Please specify file to process, and a file name of datadase to write the result')
        exit(1)

    # Set format for logger
    logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)
    # Initialize logger
    logger = logging.getLogger('Main')

    # Open compressed file with gzip with read, text option
    with gzip.open(sys.argv[1], 'rt') as file:
        logger.info('Opening file...')
        reader = FileLineReader(file)
        reader.setup()

        # Start reading
        logger.info('Processing lines from file...')
        try:
            while reader.next_line():
                try:
                    logger.info('%s' % reader.message_type)
                except InvalidFormatError as e:
                    logger.exception('Error occurred while processing a line\n%s' % e)
                    exit(1)
        except EOFError as e:
            logger.exception('Reached EOF before explicit file terminal:\n%s' % e)
            exit(1)



class Listener(protocols.Listener):
    def __init__(self, db: DatabaseWrtier, lr: FileLineReader):
        self.db = db
        self.lr = lr

    def board_start(self, data: dict):
        # Create new table
        self.db.create_table_if_not_exists(data.pair_name, database.DEF_BOARD_TABLE)

    def board_insert(self, data: dict):
        inst = dict(
            timestamp=self.lr.message_time(),
            type=database.BoardRecordType.INSERT_SELL,
            price=data.price,
            size=data.size,
        )
        self.db.insert(data.pair_name, inst)

    def board_clear(self, data: dict):
        # Complete board snapshot will delete all state in board
        inst = dict(
            timestamp=self.lr.message_time(),
            type=database.BoardRecordType.CLEAR_ALL,
            price=None,
            amount=None,
        )
        self.db.insert(data.pair_name, inst)

    def ticker_start(self, data: dict):
        self.db.create_table_if_not_exists(data.pair_name, database.DEF_TICKER_TABLE)

    def ticker_insert(self, data: dict):
        # Insert data
        inst = dict(
            timestamp=data.timestamp,
            best_bid=data.best_bid,
            best_ask=data.best_ask,
            best_bid_size=data.best_bid_size,
            best_ask_size=data.best_ask_size,
            total_bid_depth=data.total_bid_depth,
            total_ask_depth=data.total_ask_depth,
            last_traded_price=data.last_traded_price,
            volume=data.volume,
            volume_by_product=data.volume_by_product,
        )
        self.db.insert(data.pair_name, inst)

    def eos(self, pair_name: str):
        # Commit all to database
        self.db.commit()