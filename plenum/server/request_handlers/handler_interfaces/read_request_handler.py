from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


class ReadRequestHandler(RequestHandler):
    def __init__(self, node, database_manager: DatabaseManager, txn_type):
        self.node = node
        self.database_manager = database_manager
        self.txn_type = txn_type

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def get_result(self, request: Request):
        pass
