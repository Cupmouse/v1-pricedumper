import logging
import json
import datetime
import re
from enum import Enum, unique

from ..line_reader import InvalidFormatError, MessageType
from . import websocket
from .websocket import WSServiceProcessor, WebSocketProcessor

_logger = logging.getLogger('Bitflyer')

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
CHANNEL_NAME_REGEX = re.compile(r'^(lightning_board_snapshot|lightning_board|lightning_ticker|lightning_executions)_(?P<product_code>\w+)$')

@unique
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
            raise InvalidFormatError('Unknown channel "%s"' % name)


class WSSBitflyerProcessor(WSServiceProcessor):
    def setup(self, wsp: WebSocketProcessor, url: str):
        super().setup(wsp, url)
        # id vs channel map with which subscribe message it emitted
        self._emitted_subscribe_id_vs_channel = {}
        # List of channels server allowed (returned response) to above subscribe message
        # The order is time response message recieved, earliest to latest 
        self._subscribed_channels = []
        # Statuses for each channnel
        self._status = {}

    def process(self, msg: str, msg_type: MessageType):
        # If message type is EOS, messge is not in json format
        if msg_type == MessageType.EOF:
            self._process_eos()
            return
        elif msg_type == MessageType.ERR:
            return

        # Otherwise, message should be in json format
        res_obj = json.loads(msg)

        if msg_type == MessageType.EMIT:
            self._process_subscribe_emit(res_obj)
        elif msg_type == MessageType.MSG:
            # Message received, can be resut of a subscribe emit, or data
            if 'method' in res_obj:
                self._process_general_response(res_obj)
            else:
                self._process_subscribe_response(res_obj)

    def _process_subscribe_emit(self, res_obj: object):
        # Must be subscribe message
        # Check if it is
        if 'method' not in res_obj:
            raise InvalidFormatError('Emit, but a key "method" does not found in a parsed json')
        if res_obj['method'] != 'subscribe':
            raise InvalidFormatError('Emit but not a subscribe method')
            
        if 'id' not in res_obj:
            raise InvalidFormatError('Subscribe, but a key "id" does not found')
        sub_msg_id = res_obj['id']
        if sub_msg_id is None:
            raise InvalidFormatError('"id" can not be None')
        if not isinstance(sub_msg_id, int):
            raise InvalidFormatError('"id" must be an integer')

        if 'params' not in res_obj:
            raise InvalidFormatError('Subscribe method but no "params" object')
        params = res_obj['params']
        if 'channel' not in params:
            raise InvalidFormatError('A key "channel" does not found in "parameter"')
        ch_name = params['channel']
        if ch_name is None:
            raise InvalidFormatError('"channel" can not be None')
        if not isinstance(ch_name, str):
            raise InvalidFormatError('"channel" must be a string')
        
        # Register as subscribed channel to process data coming afterwards
        self._emitted_subscribe_id_vs_channel[sub_msg_id] = ch_name

    def _process_subscribe_response(self, res_obj: object):
        # It is a subscribe response
        if 'id' not in res_obj:
            raise InvalidFormatError('A response for subscribe does not have an "id" attribute')
        msg_id = res_obj['id']
        # Check if we have sent subscribe emit with the same id
        if msg_id not in self._emitted_subscribe_id_vs_channel:
            raise InvalidFormatError('Server returned a response for non existing message id %d' % msg_id)
        subject_channel = self._emitted_subscribe_id_vs_channel[msg_id]

        if 'result' not in res_obj:
            raise InvalidFormatError('A response for subscribe does not have an "result" attribute')
        if res_obj['result'] != True:
            raise InvalidFormatError('Server denied to subscribe id %s, returned result != True' % msg_id)
        # It successfully subscribed to channel with the msg id
        self._subscribed_channels.append(subject_channel)

        # Fire an event
        ch_type = ChannelType.from_channel_name(subject_channel)
        if ch_type == ChannelType.BOARD or ch_type == ChannelType.BOARD_SNAPSHOT:
            self._wsp.listener.board_start(subject_channel)
        elif ch_type == ChannelType.TICKER:
            self._wsp.listener.ticker_start(subject_channel)
        else:
            #TODO
            pass

        _logger.debug('Successfully subscribed to channel %s' % subject_channel)

    def _process_eos(self):
        _logger.info('Processing EOS...')
        # Check if it have all channels that it wanted to subscribe to, if not, it means this stream containts incomplete data
        # Set of channels it wanted to subscribe to
        emitted_channels = set(self._emitted_subscribe_id_vs_channel[i] for i in self._emitted_subscribe_id_vs_channel)
        # Set of channels server actually returned a response to a subscribe message
        subscribed_channels = set(self._subscribed_channels)
        # What channels are wanted to subscribe to but not subscribed?
        not_subscribed_channels = emitted_channels - subscribed_channels

        if len(not_subscribed_channels) > 0:
            # There are such channels at least one
            raise InvalidFormatError('Subscribe message emitted, but no response for channels %s, processed data are incomplete' % not_subscribed_channels)

        # Fire an EOS event
        self._wsp.listener.eos()

    def _process_general_response(self, res_obj: object):
        # It is data
        if res_obj['method'] != 'channelMessage':
            raise InvalidFormatError('Unknown "method" %s' % res_obj['method'])

        if 'params' not in res_obj:
            raise InvalidFormatError('"params" did not found')
        params = res_obj['params']
        if 'channel' not in params:
            raise InvalidFormatError('"channel" attribute did not found')
        channel: str = params['channel']

        if 'message' not in params:
            raise InvalidFormatError('"message" attribute did not found')

        message = params['message']

        ch_type = ChannelType.from_channel_name(channel)
        if ch_type == ChannelType.BOARD or ch_type == ChannelType.BOARD_SNAPSHOT:
            # Board information channel
            self._process_board_response(ch_type, channel, message)
        elif ch_type == ChannelType.TICKER:
            self._process_ticker_response(channel, message)
        elif ch_type == ChannelType.EXECUTIONS:
            self._process_execution_response(message)
        else:
            raise InvalidFormatError('Response of unknown channel "%s"' % channel)

    def _process_board_response(self, ch_type: ChannelType, channel_name: str, msg: object):
        self._wsp.listener.board_clear()

        # For each ask and bid, commit one record
        if 'asks' not in msg:
            raise InvalidFormatError('"asks" attribute did not found')
        if 'bids' not in msg:
            raise InvalidFormatError('"bids" attribute did not found')
        asks = msg['asks']
        bids = msg['bids']

        for ask in asks:
            if 'price' not in ask:
                raise InvalidFormatError('"price" attribute did not found')
            if 'size' not in ask:
                raise InvalidFormatError('"size" attribute did not found')
            self._wsp.listener.board_insert('ask', dict(price=ask['price'], size=ask['size']) )

        for bid in bids:
            if 'price' not in bid:
                raise InvalidFormatError('"price" attribute did not found')
            if 'size' not in bid:
                raise InvalidFormatError('"size" attribute did not found')
            self._wsp.listener.board_insert('bid', dict(price=ask['price'], size=ask['size']) )

    def _process_ticker_response(self, channel_name: str, msg: object):
        if 'product_code' not in msg:
            raise InvalidFormatError('"product_code" attribute did not found')
        
        self._wsp.listener.ticker_insert(
            dict(
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
        )

    def _process_execution_response(self, msg: object):
        self._wsp.listener.trade_insert()



# Registering websocket host name and its redirector to its parser
websocket.register_websocket_host('bitflyer.com', 'bitflyer')
websocket.register_websocket_service_redirect('bitflyer', lambda dt: WSSBitflyerProcessor)
