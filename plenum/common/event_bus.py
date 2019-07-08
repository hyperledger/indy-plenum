from collections import defaultdict
from typing import Callable, Type, Dict, List, Any, Union, Iterable


class InternalBus:
    def __init__(self):
        self._handlers = defaultdict(list)  # type: Dict[Type, List[Callable]]

    def subscribe(self, message_type: Type, handler: Callable):
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

    def subscribe(self, message_type: Type, recv_handler: RecvHandler):
        self._recv_handlers.subscribe(message_type, recv_handler)

    def send(self, message: Any, dst: Destination = None):
        self._send_handler(message, dst)

    def process_incoming(self, message: Any, frm: str):
        self._recv_handlers.send(message, frm)
