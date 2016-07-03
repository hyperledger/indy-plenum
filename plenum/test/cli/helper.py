import re

from pygments.token import Token

import plenum.cli.cli as cli
from plenum.common.txn import TXN_TYPE, TARGET_NYM, DATA, AMOUNT, CREDIT, \
    GET_BAL, GET_ALL_TXNS
from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.testable import Spyable
from plenum.test.helper import getAllArgs, checkSufficientRepliesRecvd


@Spyable(methods=[cli.Cli.print, cli.Cli.printTokens])
class TestCli(cli.Cli):
    def initializeGrammar(self):
        fcTxnsGrams = [
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>credit) \s+ (?P<amount>[0-9]+) \s+ to \s+(?P<second_client_name>[a-zA-Z0-9]+) \s*)  |",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>balance) \s*)  |",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>transactions) \s*)"
        ]
        self.clientGrams[-1] += " |"
        self.clientGrams += fcTxnsGrams
        super().initializeGrammar()

    def _clientCommand(self, matchedVars):
        if matchedVars.get('client') == 'client':
            client_name = matchedVars.get('client_name')
            client_action = matchedVars.get('cli_action')
            if client_action == "credit":
                frm = client_name
                to = matchedVars.get('second_client_name')
                toClient = self.clients.get(to, None)
                amount = int(matchedVars.get('amount'))
                txn = {
                    TXN_TYPE: CREDIT,
                    TARGET_NYM: toClient.defaultIdentifier,
                    DATA: {
                        AMOUNT: amount
                    }}
                self.sendMsg(frm, txn)
                return True
            elif client_action == "balance":
                frm = client_name
                frmClient = self.clients.get(frm, None)
                txn = {
                    TXN_TYPE: GET_BAL,
                    TARGET_NYM: frmClient.defaultIdentifier
                }
                self.sendMsg(frm, txn)
                return True
            elif client_action == "transactions":
                frm = client_name
                frmClient = self.clients.get(frm, None)
                txn = {
                    TXN_TYPE: GET_ALL_TXNS,
                    TARGET_NYM: frmClient.defaultIdentifier
                }
                self.sendMsg(frm, txn)
                return True
            else:
                return super()._clientCommand(matchedVars)

    @property
    def lastPrintArgs(self):
        args = self.printeds
        if args:
            return args[0]
        return None

    @property
    def lastPrintTokenArgs(self):
        args = self.printedTokens
        if args:
            return args[0]
        return None

    @property
    def printeds(self):
        return getAllArgs(self, TestCli.print)

    @property
    def printedTokens(self):
        return getAllArgs(self, TestCli.printTokens)

    def enterCmd(self, cmd: str):
        self.parse(cmd)


def isErrorToken(token: Token):
    return token == Token.Error


def isHeadingToken(token: Token):
    return token == Token.Heading


def isNameToken(token: Token):
    return token == Token.Name


def checkRequest(cli, looper, operation):
    cName = "Joe"
    cli.enterCmd("new client {}".format(cName))
    # Let client connect to the nodes
    looper.runFor(3)
    # Send request to all nodes
    cli.enterCmd('client {} send {}'.format(cName, operation))
    client = cli.clients[cName]
    f = getMaxFailures(len(cli.nodes))
    # Ensure client gets back the replies
    looper.run(eventually(
            checkSufficientRepliesRecvd,
            client.inBox,
            client.lastReqId,
            f,
            retryWait=2,
            timeout=30))

    txn, status = client.getReply(client.lastReqId)

    # Ensure the cli shows appropriate output
    cli.enterCmd('client {} show {}'.format(cName, client.lastReqId))
    printeds = cli.printeds
    printedReply = printeds[1]
    printedStatus = printeds[0]
    txnTimePattern = "\'txnTime\': \d+\.*\d*"
    txnIdPattern = "\'txnId\': '" + txn['txnId'] + "'"
    assert re.search(txnIdPattern, printedReply['msg'])
    assert re.search(txnTimePattern, printedReply['msg'])
    assert printedStatus['msg'] == "Status: {}".format(status)
