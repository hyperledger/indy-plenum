from abc import abstractmethod, ABCMeta

from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


class AbstractRequestManager(metaclass=ABCMeta):

    @abstractmethod
    def static_validation(self, request: Request):
        pass


class RequestManager(AbstractRequestManager):

    def __init__(self):
        self.txn_types = set()
        self.ledger_ids = set()
        self.request_handlers = {}
        self.type_to_ledger_id = {}
        self.ledger_id_to_types = {}

    def do_taa_validation(self):
        pass

    def remove_req_handler(self, txn_type):
        del self.request_handlers[txn_type]
        self.txn_types.remove(txn_type)

    def _add_handler(self, typ, handler):
        self.request_handlers[typ] = handler

    def _register_req_handler(self, handler: RequestHandler, ledger_id=None, typ=None):
        typ = typ if typ is not None else handler.txn_type
        ledger_id = ledger_id if ledger_id is not None else handler.ledger_id
        self._add_handler(typ, handler)
        self.txn_types.add(typ)
        self.type_to_ledger_id[typ] = ledger_id
        self.ledger_id_to_types.setdefault(ledger_id, set())
        self.ledger_id_to_types[ledger_id].add(typ)

    def is_valid_type(self, txn_type):
        return txn_type in self.txn_types
