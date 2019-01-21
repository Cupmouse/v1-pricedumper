import sys
import gzip
import logging
import datetime
import re
import traceback
from enum import Enum
import json


# Global logger used in everywhere
logger = None


class ProcessingError(RuntimeError):
    pass


class LineProcessor:
    def __init__(self, head: str):
        # Raw file head
        self._head = head
        # Datetime of time recorded in head
        self._head_time = None
        # Protocol (processor)
        self._protocol = None
        # Current pointed datetime
        self._pointed_datetime = None
        # Current line message type
        self._message_type = None

    def process_head(self):
        pass

    def get_pointed_datetime(self):
        return self._pointed_datetime

    def get_message_type(self):
        return self._message_type

    def get_head_datetime(self):
        return self._head_time

    def process_line(self, line: str):
        pass

    def simulate_eos(self):
        pass


class LineProcessorV0(LineProcessor):
    class MessageType(Enum):
        OPEN = 0
        MSG = 1
        EMIT = 2
        ERR = 3
        EOF = 4

        @staticmethod
        def from_str(msg_type_str: str):
            type_str_lowered = msg_type_str.lower()
            if type_str_lowered == 'open':
                return LineProcessorV0.MessageType.OPEN
            elif type_str_lowered == 'msg':
                return LineProcessorV0.MessageType.MSG
            elif type_str_lowered == 'emit':
                return LineProcessorV0.MessageType.EMIT
            elif type_str_lowered == 'error':
                return LineProcessorV0.MessageType.ERR
            elif type_str_lowered == 'eos':
                return LineProcessorV0.MessageType.EOF
            else:
                return None

    DATETIME_FORMAT_DEFAULT = '%Y-%m-%d %H:%M:%S.%f'

    HEAD_REGEX = re.compile('^0,(?P<time>[^,]+),(?P<prthead>(?P<prtname>[^,]+),(?P<prtver>[^,]+).*)$')
    LINE_REGEX = re.compile('^(?P<type>emit|msg|error|head|eos),(?P<datetime>[^,]+),(?P<msg>.+)$')

    # head,[file_version],[protocol_name],[protocol_version],...
    def process_head(self):
        logger.info('LineProcessorV0 is analyzing head...')

        '''Read head'''
        # Applying regex, check if a head is valid and get its parameters at the same time
        match_obj = self.HEAD_REGEX.match(self._head)

        if match_obj is None:
            raise ProcessingError('Header format is invalid as file head')

        head_time_str = match_obj.group('time')
        protocol_head = match_obj.group('prthead')
        protocol_name = match_obj.group('prtname')
        protocol_version_str = match_obj.group('prtver')

        # Parse an time string to a datetime instance
        head_time = datetime.datetime.strptime(head_time_str, self.DATETIME_FORMAT_DEFAULT)
        # Set pointed datetime to head recorded time since it is the earliest record in a file
        self._pointed_datetime = head_time
        logger.debug('File was opened at %s' % head_time)

        # Check if a protocol processor for a recorded name exists
        if protocol_name in PROTOCOL_PROCESSORS:
            logger.info('Protocol [%s] recognized' % protocol_name)
        else:
            raise ProcessingError('Unknown protocol [%s]' % protocol_name)

        # Parsing protocol version to int from string
        protocol_version = int(protocol_version_str)

        # Check if a protocol processor of given version exists
        if protocol_version in PROTOCOL_PROCESSORS[protocol_name]:
            logger.info('Protocol version [%d] recognized' % protocol_version)
        else:
            raise ProcessingError('Unknown protocol version [%d]' % protocol_version)

        # Initializing a protocol processor for an acquired information
        logger.info('Initializing a protocol instance...')
        self._protocol = PROTOCOL_PROCESSORS[protocol_name][protocol_version](self, protocol_head)

        # Let a protocol interpreter process its head
        self._protocol.process_head()

    def process_line(self, line: str):
        # Get message and its attributes
        match_obj = self.LINE_REGEX.match(line)
        if match_obj is None:
            raise ProcessingError('Invalid line format as line file')
        type_str = match_obj.group('type')
        datetime_str = match_obj.group('datetime')
        msg = match_obj.group('msg')

        # Convert datetime string to actual datetime instance
        line_datetime = datetime.datetime.strptime(datetime_str, self.DATETIME_FORMAT_DEFAULT)

        # Update current pointed datetime only if this line is AHEAD of last time recorded
        if (line_datetime - self._pointed_datetime) / datetime.timedelta(microseconds=1) < 0:
            # Time recorded in this line is behind of last line or whatever, but time must not rewind itself?!
            # Do not update pointed datetime so that result will be ordered with datetime
            # This and further lines with time behind of last recorded time will be regarded as all happened at the same
            # time
            logger.warn('Recorded time is going back, maybe because of system clock change?')
        else:
            # Update pointed datetime
            self._pointed_datetime = line_datetime

        # Set current line message type
        message_type = self.MessageType.from_str(type_str)
        if message_type is None:
            raise ProcessingError('Line message type %s is unknown' % type_str)
        self._message_type = message_type

        # Let protocol process a message
        self._protocol.process_line(msg)

    def simulate_eos(self):
        # Set message type as EOS
        self._message_type = LineProcessorV0.MessageType.EOF
        # Do not update _pointed_datetime
        # Call protocol processor
        self._protocol.process_line('None')


class LineFileProtocolProcessor:
    def __init__(self, line_p: LineProcessor, head: str):
        self._line_processor = line_p
        self._head = head
        self._service_processor = None

    def process_line(self, line: str):
        # Just pass it
        self._service_processor.process_line(line)

    def get_line_processor(self):
        return self._line_processor


class LFWebSocketProcessorV0(LineFileProtocolProcessor):
    HEAD_REGEX = re.compile('^websocket,0,(?P<url>.+)$')
    URL_REGEX = re.compile('^(http|https|ws|wss)://.+\.(?P<host>.+?\..+?)/.*$')

    def process_head(self):
        # Retrive parameters from head
        match_obj = self.HEAD_REGEX.match(self._head)
        if match_obj is None:
            raise ProcessingError('Invalid head as websocket version 0')

        url = match_obj.group('url')
        # Check if url is valid
        url_match = self.URL_REGEX.match(url)
        if url_match is None:
            raise ProcessingError('URL is un-parsable: %s' % url)

        # Get service instance
        host_name = url_match.group('host')
        if host_name not in WEBSOCKET_HOST_VS_SERVICE:
            raise ProcessingError('WebSocket service tied to host %s did not found' % host_name)
        service_name = WEBSOCKET_HOST_VS_SERVICE[host_name]
        # Get appropriate service class according to datetime (considering web service version)
        if service_name not in WEBSOCKET_V0_SERVICE_REDIRECT:
            raise ProcessingError('WebSocket service %s did not found' % service_name)
        service_class = WEBSOCKET_V0_SERVICE_REDIRECT[service_name](self._line_processor.get_head_datetime())
        if service_class is None:
            raise ProcessingError('Appropriate service processor did not found')

        # Make instance of service class and initialize
        self._service_processor = service_class(self, url)
        self._service_processor.initialize()


class LFWSService:
    def __init__(self, prot_p: LineFileProtocolProcessor, url: str):
        self._protocol_processor = prot_p
        self._url = url

    def initialize(self):
        pass

    def process_line(self, l: str):
        pass

    def get_protocol_processor(self):
        return self._protocol_processor

    def get_line_processor(self):
        if self._protocol_processor is None:
            raise ProcessingError('Line File WebSocket Service does not have parent protocol processor')
        return self._protocol_processor.get_line_processor()


class LFWSServiceBitflyer(LFWSService):
    def __init__(self, prop_p: LineFileProtocolProcessor, url: str):
        super().__init__(prop_p, url)
        # id vs channel map with which subscribe message it emitted
        self._emitted_subscribe_id_vs_channel = {}
        # List of channels server allowed (returned response) to above subscribe message
        # The order is time response message recieved, earliest to latest 
        self._subscribed_channels = []

    def process_line(self, l: str):
        lp = self.get_line_processor()

        # If message type is EOS, messge is not in json format
        if lp.get_message_type() == LineProcessorV0.MessageType.EOF:
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
            return

        # Otherwise, message should be in json format
        msg = json.loads(l)

        if lp.get_message_type() == LineProcessorV0.MessageType.EMIT:
            # Must be subscribe message
            # Check if it is
            if 'method' not in msg:
                raise ProcessingError('Emit, but a key "method" does not found in a parsed json')
            if msg['method'] != 'subscribe':
                raise ProcessingError('Emit but not a subscribe method')
                
            if 'id' not in msg:
                raise ProcessingError('Subscribe, but a key "id" does not found')
            sub_msg_id = msg['id']
            if sub_msg_id is None:
                raise ProcessingError('"id" can not be None')
            if not isinstance(sub_msg_id, int):
                raise ProcessingError('"id" must be an integer')

            if 'params' not in msg:
                raise ProcessingError('Subscribe method but no "params" object')
            params = msg['params']
            if 'channel' not in params:
                raise ProcessingError('A key "channel" does not found in "parameter"')
            i = params['channel']
            if i is None:
                raise ProcessingError('"channel" can not be None')
            if not isinstance(i, str):
                raise ProcessingError('"channel" must be a string')
            
            # Register as subscribed channel to process data coming afterwards
            self._emitted_subscribe_id_vs_channel[sub_msg_id] = i
        elif lp.get_message_type() == LineProcessorV0.MessageType.MSG:
            # Message received, can be resut of a subscribe emit, or data
            if 'method' in msg:
                # It is data
                if msg['method'] == 'channelMessage':
                    pass
                else:
                    raise ProcessingError('Unknown "method" %s' % msg['method'])
            else:
                # It is a subscribe response
                if 'id' not in msg:
                    raise ProcessingError('A response for subscribe does not have an "id" attribute')
                msg_id = msg['id']
                # Check if we have sent subscribe emit with the same id
                if msg_id not in self._emitted_subscribe_id_vs_channel:
                    raise ProcessingError('Server returned a response for non existing message id %d' % msg_id)
                subject_channel = self._emitted_subscribe_id_vs_channel[msg_id]

                if 'result' not in msg:
                    raise ProcessingError('A response for subscribe does not have an "result" attribute')
                if msg['result'] != True:
                    raise ProcessingError('Server denied to subscribe id %s, returned result != True' % msg_id)
                # It successfully subscribed to channel with the msg id
                self._subscribed_channels.append(subject_channel)
                logger.debug('Successfully subscribed to channel %s' % subject_channel)

# Map of line processors for a version
LINE_PROCESSORS = {
    0: LineProcessorV0,
}


WEBSOCKET_HOST_VS_SERVICE = {
    'bitflyer.com': 'bitflyer',
    'bitmex.com': 'bitmex',
    'bitfinex.com': 'bitfinex',
}


# Map of WebSocket service format processor for its name
WEBSOCKET_V0_SERVICE_REDIRECT = {
    'bitflyer': lambda dt: LFWSServiceBitflyer,
    'bitmex': lambda dt: None,
    'bitfinex': lambda dt: None,
}


# Map of protocol processor for its name
PROTOCOL_PROCESSORS = {
    'websocket': {
        0: LFWebSocketProcessorV0,
    }
}

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Please specify file to process')
        exit(1)

    # Set format for logger
    logging.basicConfig(format='[%(asctime)s][%(levelname)s] %(message)s', level=logging.INFO)
    # Initialize logger
    logger = logging.getLogger('Loader')

    # Open compressed file with gzip with read, text option, using handy with statement
    with gzip.open(sys.argv[1], 'rt') as file:
        # Check if opened file has content
        line = file.readline()

        if not line:
            logger.error('Blank file')
            exit(1)
        elif not line.startswith('head'):
            logger.error('Head line missing')
            exit(1)

        # Extract head parameters necessary to recognize a file version
        match = re.match('^head,(?P<filehead>(?P<filever>\d+?).*)$', line)

        if match is None:
            logger.error('Head format is invalid as line file')
            exit(1)

        file_head = match.group('filehead')
        version_str = match.group('filever')

        file_version = int(version_str)

        if file_version in LINE_PROCESSORS:
            logger.info('Line format version [%d] recognized' % file_version)
        else:
            logger.error('Unknown file format version [%s]' % file_version)
            exit(1)

        # Create new instance of desired file processor, giving head
        line_processor = LINE_PROCESSORS[file_version](file_head)

        try:
            line_processor.process_head()
        except ProcessingError as e:
            logger.exception('Head is invalid\n%s' % e)
            exit(1)

        logger.info('Head is valid')

        # Processing begin
        logger.info('Starting processing lines from file...')
        try:
            line = file.readline()

            while line:
                try:
                    line_processor.process_line(line)
                except ProcessingError as e:
                    logger.exception('Error occurred during processing line\n%s' % e)
                    exit(1)

                line = file.readline()
        except EOFError as e:
            logger.exception('Reached EOF before explicit file terminal:\n%s' % e)
            logger.warn('Simulating EOF instead...')
            try:
                line_processor.simulate_eos()
            except ProcessingError as e2:
                logger.exception('Simulating EOF failed\n%s' % e2)
            exit(1)
