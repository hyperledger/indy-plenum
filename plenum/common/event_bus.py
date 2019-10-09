from collections import defaultdict
from functools import partial
from typing import Callable, Type, Dict, List, Any, Union

from plenum.common.router import Router


class InternalBus(Router):
    def send(self, message: Any, *args):
        self._route(message, *args)


class ExternalBus(Router):
    Destination = Union[None, str, List[str]]
    SendHandler = Callable[[Any, Destination], None]
    RecvHandler = Callable[[Any, str], None]

    def __init__(self, send_handler: SendHandler):
        super().__init__()
        self._send_handler = send_handler

        # list of connected nodes
        self._connecteds = {}

    @property
    def connecteds(self):
        return self._connecteds

    def send(self, message: Any, dst: Destination = None):
        self._send_handler(message, dst)

    def process_incoming(self, message: Any, frm: str):
        self._route(message, frm)

    def update_connecteds(self, connecteds: dict):
        self._connecteds = connecteds
