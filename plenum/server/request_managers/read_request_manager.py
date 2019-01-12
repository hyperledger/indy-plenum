from typing import Dict

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE
from plenum.common.messages.node_messages import RequestNack
from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class ReadRequestManager(RequestManager):
    def __init__(self):
        self.request_handlers = {}  # type: Dict[int,ReadRequestHandler]

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def register_req_handler(self, handler: ReadRequestHandler):
        if not isinstance(handler, ReadRequestHandler):
            raise LogicError
        type = handler.txn_type
        self.request_handlers[type] = handler

    def get_result(self, request: Request):
        handler = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handler is None:
            return RequestNack(request.identifier, request.reqId)
        return handler.get_result(request)
