from typing import Dict
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


# TODO: Create a combined plugin for Validation or processing or create a plugin
#  package that is always distributed together
class AuctionReqProcessorPlugin:
    pluginType = "PROCESSING"

    validTxnTypes = [AUCTION_START, AUCTION_END, PLACE_BID, GET_BAL]

    def __init__(self):
        self.count = 0

        # TODO: NEED SOME WAY TO INTEGRATE PERSISTENCE IN PLUGIN

    def process(self, request):
        result = {}
        if request.operation.get(TXN_TYPE) in (AUCTION_START, AUCTION_END,
                                               PLACE_BID, GET_BAL):
            raise NotImplementedError
            # frm = request.identifier
            # if frm not in self.balances:
            #     self.balances[frm] = self.STARTING_BALANCE
            # if request.operation.get(TXN_TYPE) == CREDIT:
            #     to = request.operation[TARGET_NYM]
            #     if to not in self.balances:
            #         self.balances[to] = self.STARTING_BALANCE
            #     amount = request.operation[DATA][AMOUNT]
            #     if amount > self.balances[frm]:
            #         result[SUCCESS] = False
            #     else:
            #         result[SUCCESS] = True
            #         self.balances[to] += amount
            #         self.balances[frm] -= amount
            #         self.txns.append((frm, to, amount))
            # elif request.operation.get(TXN_TYPE) == GET_BAL:
            #     result[SUCCESS] = True
            #     result[BALANCE] = self.balances.get(frm, 0)
            # elif request.operation.get(TXN_TYPE) == GET_ALL_TXNS:
            #     result[SUCCESS] = True
            #     result[ALL_TXNS] = [txn for txn in self.txns if frm in txn]
            #
            # self.count += 1
        else:
            print("Unknown transaction type")
        return result

