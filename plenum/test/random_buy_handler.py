from _sha256 import sha256

from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from, get_req_id
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.test.buy_handler import BuyHandler
from plenum.test.constants import BUY, RANDOM_BUY
from stp_core.common.log import getlogger

logger = getlogger()


class RandomBuyHandler(BuyHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager)
        self.txn_type = RANDOM_BUY
