import traceback
import threading
import os
from enum import Enum
import datetime
import time
import logging
import gzip
import sys

import websocket
import json
import urllib.request


DUMPER_VERSION = 'after_gzing'

'''

Do not use ws.send, use self.send_message instead.
self.send_message emits event for a listener

'''


'''Utilities'''


# Enum for Listener class, lists message type
class EventType(Enum):
    OPEN = 0
    MSG = 1
    EMIT = 2
    ERR = 3
    EOF = 4


# Listener serves some functions when caller passes messages by the function on_event
class Listener:
    def on_event(self, call_type, message):
        pass


# Listener which saves messages to file
class FileWriteListener(Listener):
    # File format version of this listener, if file format changes, increment this value
    FILE_WRITE_LISTENER_VERSION = 0
    NEW_FILE_INTERVAL = 24  # Hours
    # Datetime format
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, directory, prefix):
        self.directory = directory
        self.prefix = prefix
        # Initialize file attribute as None
        self.file = None
        self.last_time_opened = None
        # Disable file reopening if True
        # self.disable_renewing = True
        # Setup logger
        self.logger = logging.getLogger('FileWriteListener/%s' % prefix)

    def close_if_not(self):
        # x and y = if x is False then not evaluate y, about y is the same
        if self.file is not None and not self.file.closed:
            self.logger.info('Closing file...')
            self.file.close()

    def open_new_file(self):
        # If file exists, and not closed, close it
        self.close_if_not()

        # Concatenate directory, prefix, datetime, and proper extention into final file path
        now = datetime.datetime.utcnow()
        formatted_datetime = now.strftime('%Y_%m_%d_%H_%M_%S')
        file_path = self.directory + self.prefix + '.' + formatted_datetime + '.json.lines.gz'

        # Making directories if not exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        self.logger.info('Opening file %s' % file_path)

        # Opening file
        self.file = gzip.open(file_path, 'at')

        # Record open time
        self.last_time_opened = now

    def on_event(self, call_type, message):
        datetimenow = datetime.datetime.utcnow()

        if call_type == EventType.MSG or call_type == EventType.EMIT or call_type == EventType.ERR:
            # Received meaningful message, record it

            # Before writing to file instance, check if it's opened, if not, open new file
            # Calculate time difference between now and last file open,
            # subtraction of datetime will produce datetime.timedelta
            # by dividing it with timedelta having attribute 1 hours produces time difference in hours
            if (datetimenow - self.last_time_opened) / datetime.timedelta(hours=1)\
                    >= self.NEW_FILE_INTERVAL:
                # This will reopen a new file in another name
                self.open_new_file()
            elif self.file.closed:
                # Reopen file (in another name)
                self.logger.error('File already closed or not yet opened!')
                self.logger.info(message)
                return

            if call_type == EventType.MSG:
                self.file.write('msg,%s,%s\n' % (datetimenow.strftime(self.DATETIME_FORMAT), message))
            elif call_type == EventType.EMIT:
                self.file.write('emit,%s,%s\n' % (datetimenow.strftime(self.DATETIME_FORMAT), message))
            elif call_type == EventType.ERR:
                self.file.write('error,%s,%s\n' % (datetimenow.strftime(self.DATETIME_FORMAT), message))
        elif call_type == EventType.OPEN:
            # Beginning of a new file
            self.open_new_file()
            self.file.write('head,%d,%s,%s\n' % (self.FILE_WRITE_LISTENER_VERSION, datetimenow.strftime(self.DATETIME_FORMAT), message))
        elif call_type == EventType.EOF:
            # Stream from caller is ended, we can no longer expect any more messages, closing file
            if not self.file.closed:
                self.file.write('eos,%s,%s\n' % (datetimenow, message))
            self.close_if_not()
        else:
            raise RuntimeError('got unknown type ' + call_type)

    def __del__(self):
        # If file is not closed yet, close it and disappear
        self.close_if_not()


# Dumper receives stream from somewhere else (usually from internet), and send it to listener
class Dumper:
    def __init__(self):
        self._listener = None
        self.logger = self.create_logger()

    def create_logger(self):
        return None

    @property
    def listener(self):
        return self._listener

    @listener.setter
    def listener(self, listener):
        self._listener = listener

    def call_listener(self, call_type, message):
        try:
            self._listener.on_event(call_type, message)
        except:
            self.logger.error('encountered an error in listener handling')
            traceback.print_exc()

    def do_dump(self):
        pass


# Dumps WebSocket stream
class WebSocketDumper(Dumper):
    # Message format version of this dumper, increase this if format changes
    WEB_SOCKET_DUMPER_VERSION = 0
    DEFAULT_RECONNECTION_TIME = 1  # Default reconnection time is 1 second
    MAX_RECONNECTION_TIME = 60  # Reconnection time will not be more than this value

    def __init__(self):
        super().__init__()
        # Current WebSocketApp for serving WebSocket stream
        self.ws_app = None
        # Last disconnection time
        self.last_disconnect = None
        # Time interval for current reconnection
        self.reconnection_time = self.DEFAULT_RECONNECTION_TIME
        # Number of disconnection in short period of time
        self.disconnection_count = 0

    def subscribe(self, ws):
        pass

    def get_url(self):
        return None

    def send_message(self, ws, message):
        ws.send(message)
        self.call_listener(EventType.EMIT, message)

    def do_dump(self):
        # Get URL for target WebSocket stream
        url = self.get_url()

        def on_open(ws):
            self.logger.info('WebSocket opened for [%s]' % url)

            # Calling a listener
            self.call_listener(EventType.OPEN, 'websocket,%d,%s' % (self.WEB_SOCKET_DUMPER_VERSION, url))

            try:
                # Do subscribing process
                self.subscribe(ws)
            except:
                self.logger.error('Encountered an error when sending subscribing message')
                traceback.print_exc()

        def on_close(ws):
            self.logger.warn('WebSocket closed for [%s]' % url)
            self.call_listener(EventType.EOF, None)

        def on_message(ws, message):
            self.call_listener(EventType.MSG, message)

        def on_error(ws, error):
            self.logger.error('Got WebSocket error [%s]:' % url)
            self.logger.error(error)

            self.call_listener(EventType.ERR, str(error))

            try:
                ws.close()
            except:
                self.logger.error('ws.close() failed')
                traceback.print_exc()

        try:
            while True:
                # If last disconnect timestamp was set, and that timestamp was within 5 seconds from now,
                # it recognize as short period connection trial and wait certain seconds
                # for not repeatedly connecting to the target server
                if (self.last_disconnect is not None) and\
                        ((self.last_disconnect - datetime.datetime.utcnow()) / datetime.timedelta(seconds=1) <= 5):
                    if self.disconnection_count != 0:
                        # Must wait more than before
                        # Set reconnection time as twice the time as before
                        self.reconnection_time *= 2
                        # Maximum reconnection time is MAX_RECONNECTION_TIME
                        if self.reconnection_time > self.MAX_RECONNECTION_TIME:
                            self.reconnection_time = self.MAX_RECONNECTION_TIME

                        # Wait
                        self.logger.warn('Waiting %d seconds for [%s]...' % (self.reconnection_time, url))
                        time.sleep(self.reconnection_time)

                    # Increase disconnection count
                    self.disconnection_count += 1
                else:
                    # Reset disconnection information
                    self.disconnection_count = 0
                    self.reconnection_time = self.DEFAULT_RECONNECTION_TIME

                self.logger.info('Connecting to [%s]...' % url)

                # Open connection to target WebSocket server
                self.ws_app = websocket.WebSocketApp(url,
                                                     on_open=on_open,
                                                     on_message=on_message,
                                                     on_error=on_error,
                                                     on_close=on_close)
                self.ws_app.run_forever()

                # Take disconnection timestamp
                self.last_disconnect = datetime.datetime.utcnow()
        except KeyboardInterrupt:
            self.logger.warn('Got kill command, exiting main loop for [%s]...' % url)
            return


'''Dumper for various exchanges'''


# This extends Dumper class and has set_listener function
class BitflyerDumper(WebSocketDumper):
    # Prefixes for individual channel
    BITFLYER_CHANNEL_PREFIXES = [
        'lightning_executions_',
        'lightning_board_snapshot_',
        'lightning_board_',
        'lightning_ticker_',
    ]

    def __init__(self):
        super().__init__()
        self.product_codes = None

    def create_logger(self):
        return logging.getLogger('Bitflyer')

    def get_url(self):
        return 'wss://ws.lightstream.bitflyer.com/json-rpc'

    def subscribe(self, ws):
        # Sending subscribe call to the server
        subscribe_obj = dict(
            method='subscribe',
            params=dict(
                channel=None
            ),
            id=None,
        )

        curr_id = 1

        # Send subscribe message
        for product_code in self.product_codes:
            for prefix in self.BITFLYER_CHANNEL_PREFIXES:
                subscribe_obj['params']['channel'] = '%s%s' % (prefix, product_code)
                subscribe_obj['id'] = curr_id
                curr_id += 1
                self.send_message(ws, json.dumps(subscribe_obj))

    def do_dump(self):
        # Get markets
        request = urllib.request.Request('https://api.bitflyer.com/v1/markets')
        with urllib.request.urlopen(request) as response:
            markets = json.load(response)

            # Response is like [{'product_code':'BTC_JPY'},{...}...]
            # Convert it to an array of 'product_code'
            self.product_codes = [obj['product_code'] for obj in markets]

        super().do_dump()


class BitmexDumper(WebSocketDumper):
    def get_url(self):
        return 'wss://www.bitmex.com/realtime?subscribe=announcement,chat,connected,funding,' \
               'instrument,insurance,liquidation,orderBookL2,publicNotifications,settlement,trade,liquidation'

    def create_logger(self):
        return logging.getLogger('Bitmex')


class BitfinexDumper(WebSocketDumper):
    # Amount of channels Bitfinex allows to open at maximum
    BITFINEX_CHANNEL_LIMIT = 30

    def __init__(self):
        super().__init__()
        self.sub_symbols = None

    def get_url(self):
        return 'wss://api.bitfinex.com/ws/2'

    def create_logger(self):
        return logging.getLogger('Bitfinex')

    def do_dump(self):
        # Before starting dumping, bitfinex has too much currencies so it has channel limitation
        # of some channels, we must cherry pick the best one to observe it's trade
        # Realize this by retrieving trading volumes for each symbol, and pick coins which volume is in the most 250

        self.logger.info('Retrieving market volumes')

        request = urllib.request.Request('https://api.bitfinex.com/v2/tickers?symbols=ALL')
        with urllib.request.urlopen(request) as response:
            tickers = json.load(response)

            # Take only normal exchange symbol which starts from 't', not funding symbol, 'f'
            # Symbol name is located at index 0
            tickers = list(filter(lambda arr: arr[0].startswith('t'), tickers))

            # Volume is NOT in USD, example, tETHBTC volume is in BTC
            # Must convert it to USD in order to sort them by USD volume
            # For this, let's make a price table
            # Last price are located at index 7
            price_table = {arr[0]: arr[7] for arr in tickers}

            # Convert raw volume to USD volume
            # tXXXYYY (volume in XXX, price in YYY)
            # if tXXXUSD exist, then volume is (volume of tXXXYYY) * (price of tXXXUSD)
            def usd_mapper(arr):
                # Symbol name
                symbol_name = arr[0]
                # Raw volume
                volume_raw = arr[8]
                # Volume in USD
                volume = 0

                # Take XXX of tXXXYYY
                pair_base = arr[0][1:4]

                if 't%sUSD' % pair_base in price_table:
                    volume = volume_raw * price_table['t%sUSD' % pair_base]
                else:
                    print('could not find proper market to calculate volume for symbol: ' + symbol_name)

                # Map to this array format
                return [symbol_name, volume]
            # Map using usd_mapper function above
            itr = map(usd_mapper, tickers)
            # Now itr (Iterator) has format of
            # [ ['tXXXYYY', 10000], ['tZZZWWW', 20000], ... ]

            # Sort iterator by USD volume using sorted().
            # Note it requires reverse option, since we are looking for symbols
            # which have the most largest volume
            itr = sorted(itr, key=lambda arr: arr[1], reverse=True)

            # Take only symbol, not an object
            itr = map(lambda ticker: ticker[0], itr)

            # Trim it down to fit a channel limit
            self.sub_symbols = list(itr)[:self.BITFINEX_CHANNEL_LIMIT//2]

        self.logger.info('Retrieving Done')

        # Call parent's do_dump
        super().do_dump()

    def subscribe(self, ws):
        subscribe_obj = dict(
            event='subscribe',
            channel=None,
            symbol=None,
        )

        # Subscribe to trades channel
        subscribe_obj['channel'] = 'trades'

        for symbol in self.sub_symbols:
            subscribe_obj['symbol'] = symbol
            self.send_message(ws, json.dumps(subscribe_obj))

        subscribe_obj['channel'] = 'book'

        for symbol in self.sub_symbols:
            subscribe_obj['symbol'] = symbol
            self.send_message(ws, json.dumps(subscribe_obj))


'''Main'''


def do_dump_bitmex():
    bm = BitmexDumper()
    bm.listener = FileWriteListener('./bitmex/', 'bitmex')
    bm.do_dump()

def do_dump_bitflyer():
    bf = BitflyerDumper()
    bf.listener = FileWriteListener('./bitflyer/', 'bitflyer')
    bf.do_dump()

def do_dump_bitfinex():
    bf = BitfinexDumper()
    bf.listener = FileWriteListener('./bitfinex/', 'bitfinex')
    bf.do_dump()

DUMPERS = {
    'bitmex': do_dump_bitmex,
    'bitflyer': do_dump_bitflyer,
    'bitfinex': do_dump_bitfinex,
}

if __name__ == '__main__':
    # Setting config format
    logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)

    logger = logging.getLogger('Main')

    logger.info('Ver [%s] starting now...', DUMPER_VERSION)

    if len(sys.argv) != 2:
        logger.error('Parameter needed')
        exit(1)

    if sys.argv[1] not in DUMPERS:
        logger.error('Invalid parameter')
        exit(1)

    dumper = DUMPERS[sys.argv[1]]

    thread = threading.Thread(target=dumper)
    thread.start()

