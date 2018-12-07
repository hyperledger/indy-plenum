from abc import abstractmethod

from plenum.server.req_handlers.handler_interfaces.handler import Handler


class TransactionHandler(Handler):

    @abstractmethod
    def static_validation(self, identifier, req_id, operation):
        pass

    @abstractmethod
    def dynamic_validation(self, request):
        pass

    @abstractmethod
    def apply_txn(self, txn, is_committed):
        pass
