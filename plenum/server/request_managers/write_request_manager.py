from typing import Dict, List

from common.exceptions import LogicError

from plenum.common.constants import TXN_TYPE

from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class WriteRequestManager(RequestManager):
    def __init__(self):
        self.request_handlers = {}  # type: Dict[int,List[WriteRequestHandler]]
        self.batch_handlers = {}

    def register_req_handler(self, handler: WriteRequestHandler):
        if not isinstance(handler, WriteRequestHandler):
            raise LogicError

        type = handler.txn_type
        if type not in self.request_handlers:
            self.request_handlers[type] = []
        self.request_handlers[type].append(handler)

    def remove_req_handlers(self, txn_type):
        del self.request_handlers[txn_type]

    def static_validation(self, request: Request):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.static_validation(request)

    def dynamic_validation(self, request: Request):
        handlers = self.request_handlers.get(request.operation[TXN_TYPE], None)
        if handlers is None:
            raise LogicError
        for handler in handlers:
            handler.dynamic_validation(request)

    def apply_request(self):
        pass

    def revert_request(self):
        pass

    def post_apply_batch(self):
        pass

    def commit_batch(self):
        pass

    def revert_batch(self):
        pass
