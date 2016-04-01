import re

from pygments.token import Token

import plenum.cli.cli as cli
from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.testable import Spyable
from plenum.test.helper import getAllArgs, checkSufficientRepliesRecvd


@Spyable(methods=[cli.Cli.print, cli.Cli.printTokens])
class TestCli(cli.Cli):

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
    timePattern = "\'time\': \d+\.*\d*"
    txnIdPattern = "\'txnId\': '" + txn['txnId'] + "'"
    txnPattern1 = "Reply for the request: \{" + timePattern + ", " + txnIdPattern + "\}"
    txnPattern2 = "Reply for the request: \{" + txnIdPattern + ", " + timePattern + "\}"
    assert re.match(txnPattern1, printedReply['msg']) or \
           re.match(txnPattern2, printedReply['msg'])
    assert printedStatus['msg'] == "Status: {}".format(status)
