from plenum.server.req_handlers.handler_interfaces.request_handler import TransactionHandler


class NymHandler(TransactionHandler):
    def static_validation(self, identifier, req_id, operation):
        pass

    def dynamic_validation(self, request):
        pass

    def apply_txn(self, txn, is_committed):
        pass
