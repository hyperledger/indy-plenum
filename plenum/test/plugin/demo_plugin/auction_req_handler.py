from plenum.common.constants import TXN_TYPE
from plenum.common.request import Request
from plenum.common.types import f
from plenum.server.req_handler import RequestHandler
from plenum.test.plugin.demo_plugin.constants import PLACE_BID, AUCTION_END, \
    AUCTION_START, GET_BAL


class AuctionReqHandler(RequestHandler):
    write_types = {AUCTION_START, AUCTION_END, PLACE_BID}
    query_types = {GET_BAL, }

    def __init__(self, ledger, state):
        super().__init__(ledger, state)
        self.query_handlers = {
            GET_BAL: self.handle_get_bal,
        }

    def get_query_response(self, request: Request):
        return self.query_handlers[request.operation[TXN_TYPE]](request)

    def handle_get_bal(self, request: Request):
        return {**request.operation, **{
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId,
        }}
