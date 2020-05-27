import datetime
from enum import Enum



class RegistryError(Exception):
    pass



class TradeType(Enum):
    BID = 0
    ASK = 1

class Listener():
    def board_start(self, pair_name: str):
        pass

    def board_insert(self, pair_name: str, type: TradeType, data: dict):
        pass

    def board_clear(self, pair_name: str):
        pass

    def ticker_start(self, pair_name: str):
        pass

    def ticker_insert(self, pair_name: str, data: dict):
        pass

    def eos(self):
        pass



class ProtocolProcessor():
    def setup(self, protocol_head: str, ref_time: datetime.datetime, listener: Listener):
        self._protocol_head = protocol_head
        self._ref_time = ref_time
        self._listener = listener

    def process(self, line: str):
        pass

    @property
    def protocol_head(self) -> str:
        return self._protocol_head

    @property
    def reference_time(self) -> datetime.datetime:
        return self._ref_time

    @property
    def listener(self) -> Listener:
        return self._listener



# Map of protocol processor for its name and version
PROTOCOL_PROCESSORS = {}

def register_protocol(name: str, version: int, clazz):
    if name in PROTOCOL_PROCESSORS:
        if version in PROTOCOL_PROCESSORS[name]:
            raise RegistryError('Protocol %s with version %d is already registered')
    else:
        PROTOCOL_PROCESSORS[name] = {}
    PROTOCOL_PROCESSORS[name][version] = clazz

def get_protocol_class(name: str, version: int):
    if (name not in PROTOCOL_PROCESSORS) or (version not in PROTOCOL_PROCESSORS[name]):
        raise RegistryError('Protocl %s with version %d is not registered')
    return PROTOCOL_PROCESSORS[name][version]



# Import all protocols
from . import websocket