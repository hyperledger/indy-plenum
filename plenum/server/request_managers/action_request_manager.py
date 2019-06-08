from typing import Dict, List

from common.exceptions import LogicError
from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.action_request_handler import ActionRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class ActionRequestManager(RequestManager):
    def __init__(self):
        self.request_handlers = {}  # type: Dict[int, ActionRequestHandler]

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def process_action(self, request: Request):
        pass

    def register_action_handler(self, handler: ActionRequestHandler):
        if not isinstance(handler, ActionRequestHandler):
            raise LogicError
        typ = handler.txn_type
        self.request_handlers[typ] = handler
