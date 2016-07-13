from typing import Dict
from typing import List
from typing import Tuple

from plenum.common.txn import TXN_TYPE, TARGET_NYM, DATA
from plenum.common.util import getlogger

logger = getlogger()


CREDIT = "CREDIT"
GET_BAL = "GET_BAL"
GET_ALL_TXNS = "GET_ALL_TXNS"
SUCCESS = "success"
BALANCE = "balance"
ALL_TXNS = "all_txns"
AMOUNT = "amount"


# TODO: Create a combined plugin for Validation or processing or create a plugin
#  package that is always distributed together
class BankReqProcessorPlugin:
    pluginType = "PROCESSING"

    validTxnTypes = [CREDIT, GET_BAL, GET_ALL_TXNS]

    STARTING_BALANCE = 1000

    def __init__(self):
        self.count = 0

        # TODO: NEED SOME WAY TO INTEGRATE PERSISTENCE IN PLUGIN

        # Balances of all client
        self.balances = {}  # type: Dict[str, int]

        # Txns of all clients, each txn is a tuple like (from, to, amount)
        self.txns = []  # type: List[Tuple]

    def process(self, request):
        result = {}
        if request.operation.get(TXN_TYPE) in (CREDIT, GET_BAL, GET_ALL_TXNS):
            frm = request.identifier
            if frm not in self.balances:
                self.balances[frm] = self.STARTING_BALANCE
            if request.operation.get(TXN_TYPE) == CREDIT:
                to = request.operation[TARGET_NYM]
                if to not in self.balances:
                    self.balances[to] = self.STARTING_BALANCE
                amount = request.operation[DATA][AMOUNT]
                if amount > self.balances[frm]:
                    result[SUCCESS] = False
                else:
                    result[SUCCESS] = True
                    self.balances[to] += amount
                    self.balances[frm] -= amount
                    self.txns.append((frm, to, amount))
            elif request.operation.get(TXN_TYPE) == GET_BAL:
                result[SUCCESS] = True
                result[BALANCE] = self.balances.get(frm, 0)
            elif request.operation.get(TXN_TYPE) == GET_ALL_TXNS:
                result[SUCCESS] = True
                result[ALL_TXNS] = [txn for txn in self.txns if frm in txn]

            self.count += 1
        else:
            print("Unknown transaction type")
        return result

