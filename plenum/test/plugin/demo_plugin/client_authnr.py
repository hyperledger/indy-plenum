from plenum.server.client_authn import CoreAuthNr
from plenum.test.plugin.demo_plugin.auction_req_handler import AuctionReqHandler


class AuctionAuthNr(CoreAuthNr):
    write_types = CoreAuthNr.write_types.union(AuctionReqHandler.write_types)
    query_types = CoreAuthNr.query_types.union(AuctionReqHandler.query_types)
