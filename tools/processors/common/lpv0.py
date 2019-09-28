# Line Processor Version 0
import processor
from processor import LineProcessor, ProcessingError, RegistryError

import re
import datetime
from enum import Enum
import logging


_logger = logging.getLogger('lpv0')

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
            return MessageType.OPEN
        elif type_str_lowered == 'msg':
            return MessageType.MSG
        elif type_str_lowered == 'emit':
            return MessageType.EMIT
        elif type_str_lowered == 'error':
            return MessageType.ERR
        elif type_str_lowered == 'eos':
            return MessageType.EOF
        else:
            return None


class LineProcessorV0(LineProcessor):
    DATETIME_FORMAT_DEFAULT = '%Y-%m-%d %H:%M:%S.%f'
    DATETIME_FORMAT_FALLBACK = '%Y-%m-%d %H:%M:%S'

    HEAD_REGEX = re.compile(r'^0,(?P<time>[^,]+),(?P<prthead>(?P<prtname>[^,]+),(?P<prtver>[^,]+).*)$')
    LINE_REGEX = re.compile(r'^(?P<type>emit|msg|error|head|eos),(?P<datetime>[^,]+),(?P<msg>.+)$')

    # head,[file_version],[protocol_name],[protocol_version],...
    def process_head(self):
        _logger.info('LineProcessorV0 is analyzing head...')

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
        _logger.debug('File was opened at %s' % head_time)

        # Check if a protocol processor for a recorded name exists
        if protocol_name in PROTOCOL_PROCESSORS:
            _logger.info('Protocol [%s] recognized' % protocol_name)
        else:
            raise ProcessingError('Unknown protocol [%s]' % protocol_name)

        # Parsing protocol version to int from string
        protocol_version = int(protocol_version_str)

        # Check if a protocol processor of given version exists
        if protocol_version in PROTOCOL_PROCESSORS[protocol_name]:
            _logger.info('Protocol version [%d] recognized' % protocol_version)
        else:
            raise ProcessingError('Unknown protocol version [%d]' % protocol_version)

        # Initializing a protocol processor for an acquired information
        _logger.info('Initializing a protocol instance...')
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
        try:
            line_datetime = datetime.datetime.strptime(datetime_str, self.DATETIME_FORMAT_DEFAULT)
        except ValueError:
            # Wierd, but if nanosecond is entirely 0, it is ommited
            line_datetime = datetime.datetime.strptime(datetime_str, self.DATETIME_FORMAT_FALLBACK)

        # Update current pointed datetime only if this line is AHEAD of last time recorded
        if (line_datetime - self._pointed_datetime) / datetime.timedelta(microseconds=1) < 0:
            # Time recorded in this line is behind of last line or whatever, but time must not rewind itself?!
            # Do not update pointed datetime so that result will be ordered with datetime
            # This and further lines with time behind of last recorded time will be regarded as all happened at the same
            # time
            _logger.warn('Recorded time is going back, maybe because of system clock change?')
        else:
            # Update pointed datetime
            self._pointed_datetime = line_datetime

        # Set current line message type
        message_type = MessageType.from_str(type_str)
        if message_type is None:
            raise ProcessingError('Line message type %s is unknown' % type_str)
        self._message_type = message_type

        # Let protocol process a message
        self._protocol.process_line(msg)

    def simulate_eos(self):
        # Set message type as EOS
        self._message_type = MessageType.EOF
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
    HEAD_REGEX = re.compile(r'^websocket,0,(?P<url>.+)$')
    URL_REGEX = re.compile(r'^(http|https|ws|wss)://.+\.(?P<host>.+?\..+?)/.*$')

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


# Map of protocol processor for its name
PROTOCOL_PROCESSORS = {
    'websocket': {
        0: LFWebSocketProcessorV0,
    }
}


WEBSOCKET_HOST_VS_SERVICE = {}

def register_websocket_host(host_name: str, service_name: str):
    if host_name in WEBSOCKET_HOST_VS_SERVICE:
        raise RegistryError('Host name "%s" is already registered' % host_name)
    WEBSOCKET_HOST_VS_SERVICE[host_name] = service_name


# Map of WebSocket service format processor for its name
WEBSOCKET_V0_SERVICE_REDIRECT = {}

def register_websocket_service_redirect(service_name: str, redirect):
    if service_name in WEBSOCKET_HOST_VS_SERVICE:
        raise RegistryError('Service name "%s" is already registered' % service_name)
    WEBSOCKET_V0_SERVICE_REDIRECT[service_name] = redirect


# Register itself (line processor)
processor.register_line_processor(0, LineProcessorV0)

import bitflyer