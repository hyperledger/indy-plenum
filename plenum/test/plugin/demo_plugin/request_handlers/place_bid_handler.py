from plenum.common.constants import DATA
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.txn_util import get_payload_data, get_from
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.constants import AUCTION_START, PLACE_BID, AMOUNT
from plenum.test.plugin.demo_plugin.request_handlers.abstract_auction_req_handler import AbstractAuctionReqHandler


class PlaceBidHandler(AbstractAuctionReqHandler):

    def __init__(self, database_manager: DatabaseManager, auctions: dict):
        super().__init__(database_manager, PLACE_BID, auctions)

    def apply_request(self, req: Request, batch_ts, prev_result):
        operation = req.operation
        data = operation.get(DATA)
        self.auctions[data['id']][req.identifier] = data[AMOUNT]

        return super().apply_request(req, batch_ts, prev_result)

    def static_validation(self, request: Request):
        self._validate_request_type(request)
        operation = request.operation
        data = operation.get(DATA)
        amount = data.get(AMOUNT)
        if not (isinstance(amount, (int, float)) and amount > 0):
            msg = '{} must be present and should be a number ' \
                  'greater than 0'.format(amount)
            raise InvalidClientRequest(request.identifier,
                                       request.reqId, msg)
        super().static_validation(request)

    def update_state(self, txn, prev_result, request, is_committed=False):
        data = get_payload_data(txn)[DATA]
        self.auctions.setdefault(data["id"], {})
        self.auctions[data["id"]][get_from(txn)] = data[AMOUNT]
        super().update_state(txn, prev_result, request, is_committed)
