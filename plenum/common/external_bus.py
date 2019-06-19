from typing import Any, Union, List, Callable, Type, Tuple

from plenum.common.event_bus import EventBus


class ExternalBus:
    Destination = Union[None, str, List[str]]
    MessageHandler = Callable[[Any, str], None]

    def __init__(self):
        self.outgoing = []  # type: List[Tuple[Any, ExternalBus.Destination]]
        self._recv_handlers = EventBus()

    def subscribe(self, message_type: Type, recv_handler: MessageHandler):
        self._recv_handlers.subscribe(message_type, recv_handler)

    def send(self, message: Any, dst: Destination = None):
        self.outgoing.append((message, dst))

    def process_incoming(self, message: Any, frm: str):
        self._recv_handlers.send(message, frm)
