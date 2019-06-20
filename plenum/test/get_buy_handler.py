from _sha256 import sha256

from common.serializers.serialization import domain_state_serializer
from plenum.common.messages.node_messages import Reply

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.common.txn_util import get_request_data, get_from, get_req_id, get_payload_data
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.test.buy_handler import BuyHandler
from plenum.test.constants import GET_BUY, BUY


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
            BUY: result
        }
        return Reply(res)

    @staticmethod
    def prepare_buy_for_state(txn):
        identifier = get_from(txn)
        req_id = get_req_id(txn)
        value = domain_state_serializer.serialize({"amount": get_payload_data(txn)['amount']})
        key = BuyHandler.prepare_buy_key(identifier, req_id)
        return key, value
