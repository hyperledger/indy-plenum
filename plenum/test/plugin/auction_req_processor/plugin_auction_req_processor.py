from types import SimpleNamespace

from plenum.cli.constants import getPipedRegEx
from plenum.common.constants import TXN_TYPE, TARGET_NYM, DATA
from plenum.common.types import PLUGIN_TYPE_PROCESSING
from stp_core.common.log import getlogger
from plenum.test.plugin.has_cli_commands import HasCliCommands

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
class AuctionReqProcessorPlugin(HasCliCommands):
    pluginType = PLUGIN_TYPE_PROCESSING
    supportsCli = True

    validTxnTypes = [AUCTION_START, AUCTION_END, PLACE_BID, GET_BAL]

    STARTING_BALANCE = 1000

    grams = [
        getPipedRegEx(pat) for pat in [
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>start\s+auction) \s+ (?P<auction_id>[a-zA-Z0-9\-]+) \s*) ",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>end\s+auction) \s+ (?P<auction_id>[a-zA-Z0-9\-]+) \s*) ",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>place\s+bid) \s+ (?P<amount>[0-9]+) \s+ on \s+(?P<auction_id>[a-zA-Z0-9\-]+) \s*) ",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>balance) \s*) "]]

    cliActionNames = {'balance', 'start auction', 'end auction', 'place bid'}

    def __init__(self):
        self.count = 0
        self._cli = None

        # TODO: NEED SOME WAY TO INTEGRATE PERSISTENCE IN PLUGIN
        # Balances of all client
        self.balances = {}  # type: Dict[str, int]
        self.auctions = {}  # type: Dict[str, SimpleNamespace]

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
                success = self.auctionExists(
                    id) and frm == self.auctions[id].creator
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
                    self._bid(id, frm, amount)
                    result.update(self.auctions[id].__dict__)
            if typ == GET_BAL:
                result[SUCCESS] = True
                result[BALANCE] = self.balances.get(frm, 0)
        else:
            print("Unknown transaction type")
        return result

    def _bid(self, auctionId, frm, bid):
        lastBidder = self.auctions[auctionId].highestBidder
        if lastBidder:
            self.balances[lastBidder] += self.auctions[auctionId].highestBid
        self.balances[frm] -= bid
        self.auctions[auctionId].highestBid = bid
        self.auctions[auctionId].highestBidder = frm

    @property
    def actions(self):
        return [self._clientAction, ]

    def _clientAction(self, matchedVars):
        if matchedVars.get('client') == 'client':
            client_name = matchedVars.get('client_name')
            client_action = matchedVars.get('cli_action')
            auctionId = matchedVars.get('auction_id')
            if client_action in ("start auction", "end auction"):
                frm = client_name
                if not self.cli.clientExists(frm):
                    self.cli.printMsgForUnknownClient()
                else:
                    txn = {
                        TXN_TYPE: AUCTION_START if client_action == "start auction"
                        else AUCTION_END,
                        DATA: {
                            ID: auctionId
                        }
                    }
                    self.cli.sendMsg(frm, txn)
                return True
            elif client_action == "place bid":
                frm = client_name
                if not self.cli.clientExists(frm):
                    self.cli.printMsgForUnknownClient()
                else:
                    amount = int(matchedVars.get('amount'))
                    txn = {
                        TXN_TYPE: PLACE_BID,
                        DATA: {
                            ID: auctionId,
                            AMOUNT: amount
                        }
                    }
                    self.cli.sendMsg(frm, txn)
                return True
            elif client_action == "balance":
                frm = client_name
                if not self.cli.clientExists(frm):
                    self.cli.printMsgForUnknownClient()
                else:
                    wallet = self.cli.wallets.get(frm, None)
                    txn = {
                        TXN_TYPE: GET_BAL,
                        TARGET_NYM: wallet.defaultId
                    }
                    self.cli.sendMsg(frm, txn)
                return True
