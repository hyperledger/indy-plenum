from types import SimpleNamespace
from typing import Dict, NamedTuple
from typing import List
from typing import Tuple

from plenum.common.txn import TXN_TYPE, TARGET_NYM, DATA
from plenum.common.util import getlogger

logger = getlogger()

AUCTION_START = "AUCTION_START"
AUCTION_END = "AUCTION_END"
PLACE_BID = "PLACE_BID"
GET_BAL = "GET_BAL"
AMOUNT = "amount"
ID = "id"
AUCTION = "auction"
CREATOR = "creator"
HIGHEST_BID = "highestBid"
HIGHEST_BIDDER = "highestBidder"
STATUS = "status"
SUCCESS = "success"
BALANCE = "balance"


# TODO: Create a combined plugin for Validation or processing or create a plugin
#  package that is always distributed together
class AuctionReqProcessorPlugin:
    pluginType = "PROCESSING"

    validTxnTypes = [AUCTION_START, AUCTION_END, PLACE_BID, GET_BAL]

    STARTING_BALANCE = 1000

    def __init__(self):
        self.count = 0

        # TODO: NEED SOME WAY TO INTEGRATE PERSISTENCE IN PLUGIN
        # Balances of all client
        self.balances = {}  # type: Dict[str, int]
        self.auctions = {} # type: Dict[str, SimpleNamespace]

    def auctionExists(self, id):
        return id in self.auctions

    def auctionLive(self, id):
        return self.auctionExists(id) and self.auctions[id].status

    def process(self, request):
        result = {}
        frm = request.identifier
        typ = request.operation.get(TXN_TYPE)
        data = request.operation.get(DATA)
        id = isinstance(data, dict) and data.get(ID)
        if frm not in self.balances:
            self.balances[frm] = self.STARTING_BALANCE
        if typ in (AUCTION_START, AUCTION_END, PLACE_BID, GET_BAL):
            if typ == AUCTION_START:
                id = data.get(ID)
                success = not self.auctionExists(id)
                result = {SUCCESS: success}
                if success:
                    # self.auctions[id] = Auction(id, frm, 0, None, 1)
                    auction = {
                        ID: id,
                        CREATOR: frm,
                        HIGHEST_BID: 0,
                        HIGHEST_BIDDER: None,
                        STATUS: 1
                    }
                    self.auctions[id] = SimpleNamespace(**auction)
                    result.update(auction)
            if typ == AUCTION_END:
                success = self.auctionExists(id) and frm == self.auctions[id].creator
                result = {SUCCESS: success}
                if success:
                    self.auctions[id].status = 0
                    result.update(self.auctions[id].__dict__)
                # TODO: Consider sending a message to the winner of the auction
            if typ == PLACE_BID:
                amount = data.get(AMOUNT)
                success = self.balances[frm] >= amount and \
                          self.auctionLive(id) \
                          and self.auctions[id].highestBid < amount
                result = {SUCCESS: success}
                if success:
                    self._settleAuction(id, frm, amount)
                    result.update(self.auctions[id].__dict__)
            if typ == GET_BAL:
                result[SUCCESS] = True
                result[BALANCE] = self.balances.get(frm, 0)
        else:
            print("Unknown transaction type")
        return result

    def _settleAuction(self, auctionId, frm, bid):
        lastBidder = self.auctions[auctionId].highestBidder
        if lastBidder:
            self.balances[lastBidder] += self.auctions[auctionId].highestBid
        self.balances[frm] -= bid
        self.auctions[auctionId].highestBid = bid
        self.auctions[auctionId].highestBidder = frm
