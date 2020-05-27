from enum import Enum
import datetime
import re
import logging



_logger = logging.getLogger('Processor')



DATETIME_FORMAT_DEFAULT = '%Y-%m-%d %H:%M:%S.%f'
DATETIME_FORMAT_FALLBACK = '%Y-%m-%d %H:%M:%S'

HEAD_REGEX = re.compile(r'^head,0,(?P<time>[^,]+),(?P<prthead>(?P<prtname>[^,]+),(?P<prtver>[^,]+).*)$')
LINE_REGEX = re.compile(r'^(?P<type>emit|msg|error|head|eos),(?P<datetime>[^,]+),(?P<msg>.+)$')



class InvalidFormatError(Exception):
    pass

class ProcessingError(Exception):
    pass



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

class Head():
    def __init__(self, head_line: str):
        self._head = head_line

        # Applying regex, check if a head is valid and get its parameters at the same time
        match_obj = HEAD_REGEX.match(head_line)

        if match_obj is None:
            raise InvalidFormatError('Header format is invalid\n%s' % head_line)

        time_str = match_obj.group('time')
        self._protocol_head = match_obj.group('prthead')
        self._protocol_name = match_obj.group('prtname')
        protocol_version_str = match_obj.group('prtver')
        if not protocol_version_str.isdecimal():
            raise InvalidFormatError('Protocol version string must be decimal, found: %s' % protocol_version_str)
        self._protocol_version = int(protocol_version_str)

        # Parse an time string to a datetime instance
        self._time = datetime.datetime.strptime(time_str, DATETIME_FORMAT_DEFAULT)

    @property
    def head(self) -> str:
        return self._head

    @property
    def protocol_head(self) -> str:
        return self._protocol_head

    @property
    def time(self) -> datetime:
        return self._time

    @property
    def protocol_name(self) -> str:
        return self._protocol_name

    @property
    def protocol_version(self) -> int:
        return self._protocol_version



from .processor import protocols
from .processor.protocols import ProtocolProcessor, Listener



class FileLineReader():
    def __init__(self, file):
        self.file = file
        self._head = None
        self._current_line = None
        self._current_time = None
        self._protocol = None

    def setup(self, listener: Listener):
        if self._head is not None:
            raise ProcessingError('Already setup')
        
        head_str = self.file.readline()

        if head_str == '':
            raise EOFError('Unexpected EOF')

        # Process head line
        self._head = Head(head_str)

        # Set current time to head time
        self._current_time = self._head.time

        # Initializing a protocol processor for an acquired information
        _logger.debug('Initializing a protocol instance: %s' % self._head._protocol_name)
        protocol_class = protocols.get_protocol_class(self._head.protocol_name, self._head.protocol_version)
        self._protocol = protocol_class()

        # Let a protocol interpreter process its head
        self._protocol.setup(self._head.protocol_head, self._current_time, listener)

    def next_line(self):
        try:
            self._current_line = self.file.readline()
        except EOFError as e:
            # Set message type as EOS
            self._message_type = MessageType.EOF
            # Do not update _pointed_datetime
            # Call protocol processor
            self._protocol.process_line('None')
            raise e
            
        # Process line
        self._process_line()
        # Check if EOF or not
        return self._current_line != ''

    @property
    def line_str(self) -> str:
        return self._current_line

    def is_EOF(self) -> bool:
        return self._current_line == ''

    def _process_line(self):
        if self.is_EOF():
            raise EOFError('File reached EOF')

        # Get message and its attributes
        match_obj = LINE_REGEX.match(self._current_line)
        if match_obj is None:
            raise InvalidFormatError('Invalid line format')
        type_str = match_obj.group('type')
        datetime_str = match_obj.group('datetime')
        msg = match_obj.group('msg')

        # Convert datetime string to actual datetime instance
        try:
            line_datetime = datetime.datetime.strptime(datetime_str, DATETIME_FORMAT_DEFAULT)
        except ValueError:
            # Wierd, but if nanosecond is entirely 0, it is ommited
            line_datetime = datetime.datetime.strptime(datetime_str, DATETIME_FORMAT_FALLBACK)
        self._raw_message_time = line_datetime
        # Update current current datetime only if this line is AHEAD of last time recorded
        if (line_datetime - self._current_time) / datetime.timedelta(microseconds=1) < 0:
            # Time recorded in this line is behind of last line or whatever, but time must not rewind itself?!
            # Do not update current datetime so that result will be ordered with datetime
            # This and further lines with time behind of last recorded time will be regarded as all happened at the same
            # time
            _logger.warn('Recorded time is going back, maybe because of system clock change?')
        else:
            # Update current datetime
            self._current_time = line_datetime

        # Set current line message type
        message_type = MessageType.from_str(type_str)
        if message_type is None:
            raise InvalidFormatError('Line message type %s is unknown' % type_str)
        self._message_type = message_type

        # Let protocol process a message
        self._protocol.process(self._message_type, msg)

    @property
    def message_type(self) -> MessageType:
        return self._message_type

    @property
    def message_time(self) -> datetime:
        return self._current_time

    @property
    def protocol(self) -> ProtocolProcessor:
        return self._protocol


