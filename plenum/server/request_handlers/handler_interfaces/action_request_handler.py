from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


class ActionRequestHandler(RequestHandler):
    def __init__(self, database_manager: DatabaseManager, txn_type, ledger_id):
        super().__init__(database_manager, txn_type, ledger_id)

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def process_action(self, request: Request):
        pass
