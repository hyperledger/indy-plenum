
from plenum.cli.constants import getPipedRegEx

from plenum.common.constants import TXN_TYPE, TARGET_NYM, DATA
from plenum.common.types import PLUGIN_TYPE_PROCESSING
from stp_core.common.log import getlogger
from plenum.test.plugin.has_cli_commands import HasCliCommands

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
class BankReqProcessorPlugin(HasCliCommands):
    pluginType = PLUGIN_TYPE_PROCESSING
    supportsCli = True

    validTxnTypes = [CREDIT, GET_BAL, GET_ALL_TXNS]
    STARTING_BALANCE = 1000

    grams = [
        getPipedRegEx(pat) for pat in [
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>credit) \s+ (?P<amount>[0-9]+) \s+ to \s+(?P<second_client_name>[a-zA-Z0-9]+) \s*) ",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>balance) \s*) ",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>transactions) \s*)"]]

    cliActionNames = {'credit', 'balance', 'transactions'}

    def __init__(self):
        self.count = 0
        self._cli = None

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

    @property
    def actions(self):
        return [self._clientAction, ]

    def _clientAction(self, matchedVars):
        if matchedVars.get('client') == 'client':
            client_name = matchedVars.get('client_name')
            client_action = matchedVars.get('cli_action')
            if client_action == "credit":
                frm = client_name
                to = matchedVars.get('second_client_name')
                toWallet = self.cli.wallets.get(to, None)
                if not self.cli.clientExists(
                        frm) or not self.cli.clientExists(to):
                    self.cli.printMsgForUnknownClient()
                else:
                    amount = int(matchedVars.get('amount'))
                    txn = {
                        TXN_TYPE: CREDIT,
                        TARGET_NYM: toWallet.defaultId,
                        DATA: {
                            AMOUNT: amount
                        }}
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
            elif client_action == "transactions":
                frm = client_name
                if not self.cli.clientExists(frm):
                    self.cli.printMsgForUnknownClient()
                else:
                    wallet = self.cli.wallets.get(frm, None)
                    txn = {
                        TXN_TYPE: GET_ALL_TXNS,
                        TARGET_NYM: wallet.defaultId
                    }
                    self.cli.sendMsg(frm, txn)
                return True
