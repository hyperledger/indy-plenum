from collections import defaultdict
from functools import partial
from typing import Callable, Type, Dict, List, Any, Union


class InternalBus:
    def __init__(self):
        self._handlers = defaultdict(list)  # type: Dict[Type, List[Callable]]

    def subscribe(self, message_type: Type, handler: Callable):
        if handler in self._handlers[message_type]:
            return
        if isinstance(handler, partial):
            for h in self._handlers[message_type]:
                if isinstance(h, partial) and h.args == handler.args:
                    return
        self._handlers[message_type].append(handler)

    def send(self, message: Any, *args):
        handlers = self._handlers[type(message)]
        for handler in handlers:
            handler(message, *args)


class ExternalBus:
    Destination = Union[None, str, List[str]]
    SendHandler = Callable[[Any, Destination], None]
    RecvHandler = Callable[[Any, str], None]

    def __init__(self, send_handler: SendHandler):
        self._send_handler = send_handler
        self._recv_handlers = InternalBus()

        # list of connected nodes
        self._connecteds = {}

    @property
    def connecteds(self):
        return self._connecteds

    def subscribe(self, message_type: Type, recv_handler: RecvHandler):
        self._recv_handlers.subscribe(message_type, recv_handler)

    def send(self, message: Any, dst: Destination = None):
        self._send_handler(message, dst)

    def process_incoming(self, message: Any, frm: str):
        self._recv_handlers.send(message, frm)

    def update_connecteds(self, connecteds: dict):
        self._connecteds = connecteds
