from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE
from plenum.common.messages.node_messages import RequestNack
from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class ReadRequestManager(RequestManager):

    def static_validation(self, request: Request):
        handler = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handler is None:
            raise LogicError
        handler.static_validation(request)

    def register_req_handler(self, handler: ReadRequestHandler, ledger_id=None):
        if not isinstance(handler, ReadRequestHandler):
            raise LogicError
        self._register_req_handler(handler, ledger_id=ledger_id)

    def get_result(self, request: Request):
        handler = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handler is None:
            return RequestNack(request.identifier, request.reqId)
        return handler.get_result(request)
