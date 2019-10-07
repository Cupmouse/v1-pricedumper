class RegistryError(Exception):
    pass



class Listener():
    def board_start(self, pair_name: str):
        pass

    def board_insert(self, pair_name: str):
        pass

    def board_clear(self, pair_name: str):
        pass

    def ticker_start(self, pair_name: str):
        pass

    def ticker_insert(self, pair_name: str):
        pass

    def eos(self):
        pass



class ProtocolProcessor():
    def setup(self, listener: Listener):
        self._listener = listener

    def process(self, line: str):
        pass

    @property
    def listener(self):
        return self._listener



# Map of protocol processor for its name and version
PROTOCOL_PROCESSORS = {}

def register_protocol(name: str, version: int, clazz):
    if (name in PROTOCOL_PROCESSORS) and (version in PROTOCOL_PROCESSORS[name]):
        raise RegistryError('Protocol %s with version %d is already registered')
    PROTOCOL_PROCESSORS[name][version] = clazz

def get_protocol_class(name: str, version: int):
    if (name not in PROTOCOL_PROCESSORS) or (version not in PROTOCOL_PROCESSORS[name]):
        raise RegistryError('Protocl %s with version %d is not registered')
    return PROTOCOL_PROCESSORS[name][version]



# Import all protocols
from . import websocket