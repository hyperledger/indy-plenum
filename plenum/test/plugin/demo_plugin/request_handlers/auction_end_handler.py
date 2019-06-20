from plenum.common.constants import DATA
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.constants import AUCTION_START, AUCTION_END
from plenum.test.plugin.demo_plugin.request_handlers.abstract_auction_req_handler import AbstractAuctionReqHandler


class AuctionEndHandler(AbstractAuctionReqHandler):

    def __init__(self, database_manager: DatabaseManager, auctions: dict):
        super().__init__(database_manager, AUCTION_END, auctions)
