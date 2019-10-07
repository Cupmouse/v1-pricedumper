import sys
import logging
import gzip
import re

from reader.line_reader import FileLineReader, InvalidFormatError

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




def pair(pair_name: str):
    # Create new table
    ch_type = ChannelType.from_channel_name(subject_channel)
    table_name = self._table_name_from_channel(subject_channel)

    if ch_type == ChannelType.BOARD or ch_type == ChannelType.BOARD_SNAPSHOT:
        db.create_table_if_not_exists(table_name, database.DEF_BOARD_TABLE)
    elif ch_type == ChannelType.TICKER:
        db.create_table_if_not_exists(table_name, database.DEF_TICKER_TABLE)
    else:
        #TODO
        pass

    # Commit all to database
    self.get_line_processor().get_database().commit()

def board_clear():
    # Complete board snapshot will delete all state in board
    if ch_type == ChannelType.BOARD_SNAPSHOT:
        data = dict(
            timestamp=ts,
            type=database.BoardRecordType.CLEAR_ALL,
            price=None,
            amount=None,
        )
        db.insert(table_name, data)

def board_insert():
    data = dict(
        timestamp=ts,
        type=database.BoardRecordType.INSERT_SELL,
        price=ask['price'],
        size=ask['size'],
    )
    db.insert(table_name, data)

def ticker_insert():
    # Convert timestamp to int
    ts = datetime.datetime.strptime(msg['timestamp'][:-2], DATETIME_FORMAT)
    # Insert data
    table_name = self._table_name_from_channel(channel_name)
    data = dict(
        timestamp=ts,
        best_bid=msg['best_bid'],
        best_ask=msg['best_ask'],
        best_bid_size=msg['best_bid_size'],
        best_ask_size=msg['best_ask_size'],
        total_bid_depth=msg['total_bid_depth'],
        total_ask_depth=msg['total_ask_depth'],
        last_traded_price=msg['ltp'],
        volume=msg['volume'],
        volume_by_product=msg['volume_by_product'],
    )
    db.insert(table_name, data)



def _table_name_from_channel(self, channel_name: str):
    ch_type = ChannelType.from_channel_name(channel_name)
    if ch_type == ChannelType.BOARD_SNAPSHOT:
        ch_type = ChannelType.BOARD
        
    match_object = CHANNEL_NAME_REGEX.match(channel_name)

    if match_object is None:
        raise InvalidFormatError('Undefined channel name')

    product_code = match_object.group('product_code')

    return '%s_%s' % (ch_type.name.lower(), product_code)