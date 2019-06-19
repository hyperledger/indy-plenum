from typing import Any, Union, List, Callable, Type

from plenum.common.event_bus import EventBus


class ExternalBus:
    Destination = Union[None, str, List[str]]
    SendHandler = Callable[[Any, Destination], None]
    RecvHandler = Callable[[Any, str], None]

    def __init__(self, send_handler: SendHandler):
        self._send_handler = send_handler
        self._recv_handlers = EventBus()

    def subscribe(self, message_type: Type, recv_handler: RecvHandler):
        self._recv_handlers.subscribe(message_type, recv_handler)

    def send(self, message: Any, dst: Destination = None):
        self._send_handler(message, dst)

    def recv(self, message: Any, frm: str):
        self._recv_handlers.send(message, frm)
