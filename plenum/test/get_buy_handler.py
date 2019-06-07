from plenum.common.messages.node_messages import Reply

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.common.txn_util import get_request_data
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.test.buy_handler import BuyHandler
from plenum.test.constants import GET_BUY


class GetBuyHandler(ReadRequestHandler):
    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, GET_BUY, DOMAIN_LEDGER_ID)

    def static_validation(self, request: Request):
        self._validate_request_type(request)

    def dynamic_validation(self, request: Request):
        self._validate_request_type(request)

    def get_result(self, request: Request):
        identifier, req_id, operation = get_request_data(request)
        buy_key = BuyHandler.prepare_buy_key(identifier, req_id)
        result = self.state.get(buy_key)

        res = {
            f.IDENTIFIER.nm: identifier,
            f.REQ_ID.nm: req_id,
            "buy": result
        }
        return Reply(res)