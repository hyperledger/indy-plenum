from typing import Callable, List, Any, Union, NamedTuple

from plenum.common.router import Router


class InternalBus(Router):
    def send(self, message: Any, *args):
        self._route(message, *args)


class ExternalBus(Router):
    Destination = Union[None, str, List[str]]
    SendHandler = Callable[[Any, Destination], None]
    RecvHandler = Callable[[Any, str], None]
    Connected = NamedTuple('Connected', [])
    Disconnected = NamedTuple('Disconnected', [])

    def __init__(self, send_handler: SendHandler):
        super().__init__()
        self._send_handler = send_handler

        # list of connected nodes
        self._connecteds = set()

    @property
    def connecteds(self) -> set:
        return self._connecteds

    def send(self, message: Any, dst: Destination = None):
        self._send_handler(message, dst)

    def process_incoming(self, message: Any, frm: str):
        self._route(message, frm)

    def update_connecteds(self, connecteds: set):
        # TODO: Make more efficient API
        connected = connecteds - self._connecteds
        disconnected = self._connecteds - connecteds
        self._connecteds = connecteds
        for frm in connected:
            self.process_incoming(self.Connected(), frm)
        for frm in disconnected:
            self.process_incoming(self.Disconnected(), frm)
