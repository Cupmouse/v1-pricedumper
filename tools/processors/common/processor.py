import sys
import gzip
import logging
import datetime
import traceback
import json
import re

import database

_logger = logging.getLogger('Processor')

class RegistryError(RuntimeError):
    pass

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
        # Database
        self._database = database.DatabaseWrtier()

    def initialize_database(self):
        self._database.connect('test.db')

    def get_database(self):
        return self._database

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


# Map of line processors for a version
LINE_PROCESSORS = {}

def register_line_processor(version: int, clz: LineProcessor):
    if version in LINE_PROCESSORS:
        raise RegistryError('Version "%d" is already registered' % version)
    LINE_PROCESSORS[version] = clz

def get_appropriate_processor(head: str):
    # Extract head parameters necessary to recognize a file version
    match = re.match(r'^head,(?P<filehead>(?P<filever>\d+?).*)$', head)

    if match is None:
        raise ProcessingError('Head format is invalid as line file')

    file_head = match.group('filehead')
    version_str = match.group('filever')

    file_version = int(version_str)

    if file_version in LINE_PROCESSORS:
        _logger.info('Line format version [%d] recognized' % file_version)
    else:
        raise ProcessingError('Unknown file format version [%s]' % file_version)

    return LINE_PROCESSORS[file_version](file_head)


import lpv0