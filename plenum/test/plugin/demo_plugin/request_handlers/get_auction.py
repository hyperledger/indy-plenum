from plenum.common.constants import DATA

from common.serializers.json_serializer import JsonSerializer
from plenum.common.request import Request
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.constants import GET_AUCTION


class GetAuctionHandler(ReadRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, GET_AUCTION, AUCTION_LEDGER_ID)

    def static_validation(self, request: Request):
        pass

    def get_result(self, request: Request):
        auction_id = request.operation.get(DATA).get("auction_id").encode()
        serialized_auction = self.state.get(auction_id, isCommitted=True)

        auction = JsonSerializer().deserialize(serialized_auction) if serialized_auction else None

        return {**request.operation, **{
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId,
            auction_id: auction
        }}
