import re
import datetime

from . import protocols 
from .protocols import ProtocolProcessor, RegistryError, Listener
from ..line_reader import InvalidFormatError, Head, MessageType



HEAD_REGEX = re.compile(r'^websocket,0,(?P<url>.+)$')
URL_REGEX = re.compile(r'^(http|https|ws|wss)://.+\.(?P<host>.+?\..+?)/.*$')



class WebSocketProcessor(ProtocolProcessor):
    def setup(self, protocol_head: str, ref_time: datetime.datetime, listener: Listener):
        super().setup(protocol_head, ref_time, listener)

        # Retrive parameters from head
        match_obj = HEAD_REGEX.match(protocol_head)
        if match_obj is None:
            raise InvalidFormatError('Invalid head as websocket version 0')

        url = match_obj.group('url')

        # Get service instance
        service_class = get_service(url, ref_time)

        # Make instance of service class and initialize
        self._service_processor = service_class()
        self._service_processor.setup(self, url)

    def process(self, msg_type: MessageType, line: str):
        self._service_processor.process(msg_type, line)




class WSServiceProcessor:
    def setup(self, wsp: WebSocketProcessor, url: str):
        self._wsp = wsp
        self._url = url

    def process(self, msg_type: MessageType, msg: str):
        pass

    @property
    def url(self):
        return self._url



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


def get_service(url: str, time: datetime.datetime):
    # Check if url is valid
    url_match = URL_REGEX.match(url)
    if url_match is None:
        raise InvalidFormatError('URL is un-parsable: %s' % url)
    # Find service class
    host_name = url_match.group('host')
    if host_name not in WEBSOCKET_HOST_VS_SERVICE:
        raise InvalidFormatError('WebSocket service tied to host %s did not found' % host_name)
    service_name = WEBSOCKET_HOST_VS_SERVICE[host_name]
    # Get appropriate service class according to datetime (considering web service version)
    if service_name not in WEBSOCKET_V0_SERVICE_REDIRECT:
        raise InvalidFormatError('WebSocket service %s did not found' % service_name)
    service_class = WEBSOCKET_V0_SERVICE_REDIRECT[service_name](time)
    if service_class is None:
        raise InvalidFormatError('Appropriate service processor did not found')

    return service_class


# Register itself as protocol
protocols.register_protocol('websocket', 0, WebSocketProcessor)



from . import bitflyer