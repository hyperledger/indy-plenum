from abc import abstractmethod, ABCMeta

from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


class AbstractRequestManager(metaclass=ABCMeta):

    @abstractmethod
    def static_validation(self, request: Request):
        pass

    @abstractmethod
    def dynamic_validation(self, request: Request):
        pass


class RequestManager(AbstractRequestManager):

    def __init__(self):
        self.txn_types = set()
        self.ledger_ids = set()
        self.request_handlers = {}

    def remove_req_handler(self, txn_type):
        del self.request_handlers[txn_type]
        self.txn_types.remove(txn_type)

    def _register_req_handler(self, handler: RequestHandler):
        typ = handler.txn_type
        self.request_handlers[typ] = handler
        self.txn_types.add(typ)

    def is_valid_type(self, txn_type):
        return txn_type in self.txn_types
