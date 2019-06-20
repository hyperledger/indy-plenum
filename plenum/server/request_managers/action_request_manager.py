from typing import Dict, List

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE
from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.action_request_handler import ActionRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class ActionRequestManager(RequestManager):

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        handler = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handler is None:
            raise LogicError
        handler.dynamic_validation(request)

    def process_action(self, request: Request):
        handler = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handler is None:
            raise LogicError
        return handler.process_action(request)

    def register_action_handler(self, handler: ActionRequestHandler):
        if not isinstance(handler, ActionRequestHandler):
            raise LogicError
        self._register_req_handler(handler)
