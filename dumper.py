import traceback
import threading
import os
from enum import Enum
import datetime
import time

import websocket
import json
import urllib.request


'''Utilities'''


# Enum for Listener class, lists message type
class EventType(Enum):
    OPEN = 0
    MSG = 1
    EOF = 2


# Listener serves some functions when caller passes messages by the function on_event
class Listener:
    def on_event(self, call_type, message):
        pass


# Listener which saves messages to file
class FileWriteListener(Listener):
    NEW_FILE_INTERVAL = 24  # Hours

    def __init__(self, directory, prefix):
        self.directory = directory
        self.prefix = prefix
        # Initialize file attribute as None
        self.file = None
        self.last_time_opened = None
        # Disable file reopening if True
        # self.disable_renewing = True
        self.open_new_file()

    def close_if_not(self):
        # x and y = if x is False then not evaluate y, about y is the same
        if self.file is not None and not self.file.closed:
            self.file.close()

    def open_new_file(self):
        # If file exists, and not closed, close it
        self.close_if_not()

        # Concatenate directory, prefix, datetime, and proper extention into final file path
        now = datetime.datetime.now()
        formatted_datetime = now.strftime('%Y_%m_%d_%H_%M_%S')
        file_path = self.directory + self.prefix + '.' + formatted_datetime + '.json.lines'

        # Making directories if not exist
        os.makedirs(self.directory)

        # Opening file
        self.file = open(file_path, 'a')

        # Record open time
        self.last_time_opened = now

    def on_event(self, call_type, message):
        if call_type == EventType.MSG:
            # Received meaningful message, record it

            # Before writing to file instance, check if it's opened, if not, open new file
            # Calculate time difference between now and last file open,
            # subtraction of datetime will produce datetime.timedelta
            # by dividing it with timedelta having attribute 1 hours produces time difference in hours
            if (datetime.datetime.now() - self.last_time_opened) / datetime.timedelta(hours=1)\
                    >= self.NEW_FILE_INTERVAL:
                # This will reopen a new file in another name
                self.open_new_file()
            elif self.file.closed:
                # Reopen file (in another name)
                print('file already closed, reopening...')
                self.open_new_file()

            self.file.write(message + '\n')
        elif call_type == EventType.OPEN:
            # It have to do nothing when stream from caller is opened
            pass
        elif call_type == EventType.EOF:
            # Stream from caller is ended, we can no longer expect any more messages, closing file
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
            print('encountered an error in listener handling')
            traceback.print_exc()

    def do_dump(self):
        pass


# Dumps WebSocket stream
class WebSocketDumper(Dumper):
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
        return 'ws://example.com'

    def do_dump(self):
        # Get URL for target WebSocket stream
        url = self.get_url()

        def on_open(ws):
            print('WebSocket opened for [%s]' % url)
            try:
                # Do subscribing process
                self.subscribe(ws)
            except:
                print('Encountered an error when sending subscribing message')
                traceback.print_exc()

            # Calling a listener
            self.call_listener(EventType.OPEN, None)

        def on_close(ws):
            print('** WebSocket closed for [%s]' % url)
            self.call_listener(EventType.EOF, None)

        def on_message(ws, message):
            self.call_listener(EventType.MSG, message)

        def on_error(ws):
            print('** Got WebSocket error [%s]:' % url)
            print(ws)
            try:
                ws.close()
            except:
                print('ws.close() failed')
                traceback.print_exc()

        print('Connecting to [%s]...' % url)

        try:
            while True:
                # If last disconnect timestamp was set, and that timestamp was within 5 seconds from now,
                # it recognize as short period connection trial and wait certain seconds
                # for not repeatedly connecting to the target server
                if (self.last_disconnect is not None) and\
                        ((self.last_disconnect - datetime.datetime.now()) / datetime.timedelta(seconds=1) <= 5):
                    # Increase disconnection count
                    self.disconnection_count += 1

                    # Must wait more than before
                    if self.reconnection_time == 0:
                        self.reconnection_time = 1
                    else:
                        # Set reconnection time as twice the time as before
                        self.reconnection_time *= 2
                        # Maximum reconnection time is MAX_RECONNECTION_TIME
                        if self.reconnection_time > self.MAX_RECONNECTION_TIME:
                            self.reconnection_time = self.MAX_RECONNECTION_TIME

                    # Wait
                    print('** Waiting %d seconds for [%s]...' % (self.reconnection_time, url))
                    time.sleep(self.reconnection_time)
                else:
                    # Reset disconnection information, if it is
                    self.last_disconnect = None
                    self.disconnection_count = 0

                # Open connection to target WebSocket server
                self.ws_app = websocket.WebSocketApp(url,
                                                     on_open=on_open,
                                                     on_message=on_message,
                                                     on_error=on_error,
                                                     on_close=on_close)
                self.ws_app.run_forever()

                # Take disconnection timestamp
                self.last_disconnect = datetime.datetime.now()
        except KeyboardInterrupt:
            print('Got kill command, exiting main loop for [%s]...' % url)
            return


'''Dumper for various exchanges'''


# This extends Dumper class and has set_listener function
class BitflyerDumper(WebSocketDumper):
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

        subscribe_obj['params']['channel'] = 'lightning_executions_FX_BTC_JPY'
        subscribe_obj['id'] = 1
        ws.send(json.dumps(subscribe_obj))

        subscribe_obj['params']['channel'] = 'lightning_executions_BTC_JPY'
        subscribe_obj['id'] = 2
        ws.send(json.dumps(subscribe_obj))

        subscribe_obj['params']['channel'] = 'lightning_executions_ETH_BTC'
        subscribe_obj['id'] = 3
        ws.send(json.dumps(subscribe_obj))

        subscribe_obj['params']['channel'] = 'lightning_executions_BCH_BTC'
        subscribe_obj['id'] = 4
        ws.send(json.dumps(subscribe_obj))


class BitmexDumper(WebSocketDumper):
    def get_url(self):
        return 'wss://www.bitmex.com/realtime?subscribe=trade'


class BitfinexDumper(WebSocketDumper):
    # Amount of channels Bitfinex allows to open at maximum
    BITFINEX_CHANNEL_LIMIT = 250

    def __init__(self):
        super().__init__()
        self.sub_symbols = None

    def get_url(self):
        return 'wss://api.bitfinex.com/ws/2'

    def do_dump(self):
        # Before starting dumping, bitfinex has too much currencies so it has channel limitation
        # of some channels, we must cherry pick the best one to observe it's trade
        # Realize this by retrieving trading volumes for each symbol, and pick coins which volume is in the most 250

        print('Bitfinex: Retrieving market volumes')

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

        print('Bitfinex: Retrieving Done')

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
            ws.send(json.dumps(subscribe_obj))

        subscribe_obj['channel'] = 'book'

        for symbol in self.sub_symbols:
            subscribe_obj['symbol'] = symbol
            ws.send(json.dumps(subscribe_obj))


'''Main'''


if __name__ == '__main__':
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

    thread_bfi = threading.Thread(target=do_dump_bitfinex)
    thread_bfl = threading.Thread(target=do_dump_bitflyer)
    thread_bim = threading.Thread(target=do_dump_bitmex)

    thread_bfi.start()
    thread_bfl.start()
    thread_bim.start()

