from plenum.common.constants import DATA
from plenum.common.exceptions import UnauthorizedClientRequest, InvalidClientRequest
from plenum.common.request import Request
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.constants import AUCTION_START, PLACE_BID, AMOUNT, GET_BAL
from plenum.test.plugin.demo_plugin.request_handlers.abstract_auction_req_handler import AbstractAuctionReqHandler


class GetBalHandler(ReadRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, GET_BAL, AUCTION_LEDGER_ID)

    def static_validation(self, request: Request):
        pass

    def get_result(self, request: Request):
        return {**request.operation, **{
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId,
        }}
