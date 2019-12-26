from _sha256 import sha256
from typing import Optional

from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from, get_req_id
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.test.constants import BUY
from stp_core.common.log import getlogger

logger = getlogger()


class BuyHandler(WriteRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, BUY, DOMAIN_LEDGER_ID)

    def static_validation(self, request: Request):
        self._validate_request_type(request)

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        self._validate_request_type(request)

    def update_state(self, txn, prev_result, request, is_committed=False):
        self._validate_txn_type(txn)
        key = self.gen_state_key(txn)
        value = domain_state_serializer.serialize({"amount": get_payload_data(txn)['amount']})
        self.state.set(key, value)
        logger.trace('{} after adding to state, headhash is {}'.
                     format(self, self.state.headHash))

    def gen_state_key(self, txn):
        identifier = get_from(txn)
        req_id = get_req_id(txn)
        return self.prepare_buy_key(identifier, req_id)

    @staticmethod
    def prepare_buy_key(identifier, req_id):
        return sha256('{}{}:buy'.format(identifier, req_id).encode()).digest()

    def __repr__(self):
        return "TestHandler"
