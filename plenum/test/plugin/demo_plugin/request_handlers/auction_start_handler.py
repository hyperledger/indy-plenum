from typing import Optional

from plenum.common.constants import DATA
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.test.plugin.demo_plugin.constants import AUCTION_START
from plenum.test.plugin.demo_plugin.request_handlers.abstract_auction_req_handler import AbstractAuctionReqHandler


class AuctionStartHandler(AbstractAuctionReqHandler):

    def __init__(self, database_manager: DatabaseManager, auctions: dict):
        super().__init__(database_manager, AUCTION_START, auctions)

    def dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        self._validate_request_type(request)
        operation = request.operation
        data = operation.get(DATA)
        self.auctions[data['id']] = {}
