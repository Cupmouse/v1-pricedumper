import logging
import json
import datetime
from enum import Enum

from processor import ProcessingError
import lpv0
from lpv0 import MessageType, LFWSService, LineFileProtocolProcessor, LineProcessorV0


_logger = logging.getLogger('Bitflyer')


class ChannelType(Enum):
    EXECUTIONS = 0
    BOARD = 1
    BOARD_SNAPSHOT = 2
    TICKER = 3

    @staticmethod
    def from_channel_name(name: str):
        if name.startswith('lightning_board_snapshot_'):
            return ChannelType.BOARD_SNAPSHOT
        elif name.startswith('lightning_board_'):
            return ChannelType.BOARD
        elif name.startswith('lightning_ticker_'):
            return ChannelType.TICKER
        elif name.startswith('lightning_executions_'):
            return ChannelType.EXECUTIONS
        else:
            raise ProcessingError('Unknown channel "%s"' % name)


class LFWSServiceBitflyer(LFWSService):
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
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
        bid_or_ask='INTEGER(1) NOT NULL',
        price='REAL NOT NULL',
        size='REAL NOT NULL'
    )

    def __init__(self, prop_p: LineFileProtocolProcessor, url: str):
        super().__init__(prop_p, url)
        # id vs channel map with which subscribe message it emitted
        self._emitted_subscribe_id_vs_channel = {}
        # List of channels server allowed (returned response) to above subscribe message
        # The order is time response message recieved, earliest to latest 
        self._subscribed_channels = []
        # Statuses for each channnel
        self._status = {}

    def process_line(self, l: str):
        lp = self.get_line_processor()

        # If message type is EOS, messge is not in json format
        if lp.get_message_type() == MessageType.EOF:
            self._process_eos()
            return

        # Otherwise, message should be in json format
        res_obj = json.loads(l)

        if lp.get_message_type() == MessageType.EMIT:
            self._process_subscribe_emit(res_obj)
        elif lp.get_message_type() == MessageType.MSG:
            # Message received, can be resut of a subscribe emit, or data
            if 'method' in res_obj:
                self._process_general_response(res_obj)
            else:
                self._process_subscribe_response(res_obj)

    def _process_subscribe_emit(self, res_obj: object):
        # Must be subscribe message
        # Check if it is
        if 'method' not in res_obj:
            raise ProcessingError('Emit, but a key "method" does not found in a parsed json')
        if res_obj['method'] != 'subscribe':
            raise ProcessingError('Emit but not a subscribe method')
            
        if 'id' not in res_obj:
            raise ProcessingError('Subscribe, but a key "id" does not found')
        sub_msg_id = res_obj['id']
        if sub_msg_id is None:
            raise ProcessingError('"id" can not be None')
        if not isinstance(sub_msg_id, int):
            raise ProcessingError('"id" must be an integer')

        if 'params' not in res_obj:
            raise ProcessingError('Subscribe method but no "params" object')
        params = res_obj['params']
        if 'channel' not in params:
            raise ProcessingError('A key "channel" does not found in "parameter"')
        ch_name = params['channel']
        if ch_name is None:
            raise ProcessingError('"channel" can not be None')
        if not isinstance(ch_name, str):
            raise ProcessingError('"channel" must be a string')
        
        # Register as subscribed channel to process data coming afterwards
        self._emitted_subscribe_id_vs_channel[sub_msg_id] = ch_name

    def _process_subscribe_response(self, res_obj: object):
        # It is a subscribe response
        if 'id' not in res_obj:
            raise ProcessingError('A response for subscribe does not have an "id" attribute')
        msg_id = res_obj['id']
        # Check if we have sent subscribe emit with the same id
        if msg_id not in self._emitted_subscribe_id_vs_channel:
            raise ProcessingError('Server returned a response for non existing message id %d' % msg_id)
        subject_channel = self._emitted_subscribe_id_vs_channel[msg_id]

        if 'result' not in res_obj:
            raise ProcessingError('A response for subscribe does not have an "result" attribute')
        if res_obj['result'] != True:
            raise ProcessingError('Server denied to subscribe id %s, returned result != True' % msg_id)
        # It successfully subscribed to channel with the msg id
        self._subscribed_channels.append(subject_channel)

        # Create new table
        db = self.get_line_processor().get_database()
        ch_type = ChannelType.from_channel_name(subject_channel)
        if ch_type == ChannelType.BOARD or ch_type == ChannelType.BOARD_SNAPSHOT:
            db.create_table_if_not_exists(subject_channel, self.DEF_BOARD_TABLE)
        elif ch_type == ChannelType.TICKER:
            db.create_table_if_not_exists(subject_channel, self.DEF_TICKER_TABLE)
        else:
            #TODO
            pass

        _logger.debug('Successfully subscribed to channel %s' % subject_channel)

    def _process_eos(self):
        # Check if it have all channels that it wanted to subscribe to, if not, it means this stream containts incomplete data
        # Set of channels it wanted to subscribe to
        emitted_channels = set(self._emitted_subscribe_id_vs_channel[i] for i in self._emitted_subscribe_id_vs_channel)
        # Set of channels server actually returned a response to a subscribe message
        subscribed_channels = set(self._subscribed_channels)
        # What channels are wanted to subscribe to but not subscribed?
        not_subscribed_channels = emitted_channels - subscribed_channels

        if len(not_subscribed_channels) > 0:
            # There are such channels at least one
            raise ProcessingError('Subscribe message emitted, but no response for channels %s, processed data are incomplete' % not_subscribed_channels)

    def _process_general_response(self, res_obj: object):
        # It is data
        if res_obj['method'] != 'channelMessage':
            raise ProcessingError('Unknown "method" %s' % res_obj['method'])

        if 'params' not in res_obj:
            raise ProcessingError('"params" did not found')
        params = res_obj['params']
        if 'channel' not in params:
            raise ProcessingError('"channel" attribute did not found')
        channel: str = params['channel']

        if 'message' not in params:
            raise ProcessingError('"message" attribute did not found')

        message = params['message']

        ch_type = ChannelType.from_channel_name(channel)

        if ch_type == ChannelType.BOARD or ch_type == ChannelType.BOARD_SNAPSHOT:
            # Board information channel
            self._process_board_response(channel, message)
        elif ch_type == ChannelType.TICKER:
            self._process_ticker_response(channel, message)
        elif ch_type == ChannelType.EXECUTIONS:
            self._process_execution_response(message)
        else:
            raise ProcessingError('Response of unknown channel "%s"' % channel)

    def _process_board_response(self, channel_name: str, msg: object):
        lp = self.get_line_processor()
        db = lp.get_database()
        
        ts = lp.get_pointed_datetime()

        if 'asks' not in msg:
            raise ProcessingError('"asks" attribute did not found')
        if 'bids' not in msg:
            raise ProcessingError('"bids" attribute did not found')
        asks = msg['asks']
        bids = msg['bids']

        for ask in asks:
            if 'price' not in ask:
                raise ProcessingError('"price" attribute did not found')
            if 'size' not in ask:
                raise ProcessingError('"size" attribute did not found')
            data = dict(
                timestamp=ts,
                bid_or_ask=0,
                price=ask['price'],
                size=ask['size']
            )
            db.insert(channel_name, data)
        for bid in bids:
            if 'price' not in bid:
                raise ProcessingError('"price" attribute did not found')
            if 'size' not in bid:
                raise ProcessingError('"size" attribute did not found')
            data = dict(
                timestamp=ts,
                bid_or_ask=1,
                price=bid['price'],
                size=bid['size']
            )
            db.insert(channel_name, data)

    def _process_ticker_response(self, channel_name: str, msg: object):
        if 'product_code' not in msg:
            raise ProcessingError('"product_code" attribute did not found')
        db = self.get_line_processor().get_database()
        
        # Convert timestamp to int
        ts = datetime.datetime.strptime(msg['timestamp'][:-2], self.DATETIME_FORMAT)
        # Insert data
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
        db.insert(channel_name, data)

    def _process_execution_response(self, msg: object):
        pass

# Registering websocket host name and its redirector to its parser
lpv0.register_websocket_host('bitflyer.com', 'bitflyer')
lpv0.register_websocket_service_redirect('bitflyer', lambda dt: LFWSServiceBitflyer)