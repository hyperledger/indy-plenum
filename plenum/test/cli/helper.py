import re

from pygments.token import Token

import plenum.cli.cli as cli
from plenum.common.txn import TXN_TYPE, TARGET_NYM, DATA
from plenum.common.util import getMaxFailures, firstValue
from plenum.test.cli.mock_output import MockOutput
from plenum.test.eventually import eventually
from plenum.test.testable import Spyable
from plenum.test.helper import getAllArgs, checkSufficientRepliesRecvd,\
    CREDIT, AMOUNT, GET_BAL, GET_ALL_TXNS, TestNode, TestClient, checkPoolReady


class TestCliCore:
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

    @property
    def lastCmdOutput(self):
        return '\n'.join([x['msg'] for x in
                          list(reversed(self.printeds))[self.lastPrintIndex::]])

    def enterCmd(self, cmd: str):
        self.lastPrintIndex = len(self.printeds)
        self.parse(cmd)

    def lastMsg(self):
        return self.lastPrintArgs['msg']


@Spyable(methods=[cli.Cli.print, cli.Cli.printTokens])
class TestCli(cli.Cli, TestCliCore):
    def initializeGrammar(self):
        fcTxnsGrams = [
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>credit) \s+ (?P<amount>[0-9]+) \s+ to \s+(?P<second_client_name>[a-zA-Z0-9]+) \s*)  |",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>balance) \s*)  |",
            "(\s* (?P<client>client) \s+ (?P<client_name>[a-zA-Z0-9]+) \s+ (?P<cli_action>transactions) \s*)"
        ]
        self.clientGrams[-1] += " |"
        self.clientGrams += fcTxnsGrams
        cli.Cli.initializeGrammar(self)

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
                return cli.Cli._clientCommand(self, matchedVars)


def isErrorToken(token: Token):
    return token == Token.Error


def isHeadingToken(token: Token):
    return token == Token.Heading


def isNameToken(token: Token):
    return token == Token.Name


def checkNodeStarted(cli, nodeName):
    # Node name should be in cli.nodes
    assert nodeName in cli.nodes

    def chk():
        msgs = {stmt['msg'] for stmt in cli.printeds}
        assert "{} added replica {}:0 to instance 0 (master)" \
                   .format(nodeName, nodeName) in msgs
        assert "{} added replica {}:1 to instance 1 (backup)" \
                   .format(nodeName, nodeName) in msgs
        assert "{} listening for other nodes at {}:{}" \
                   .format(nodeName, *cli.nodes[nodeName].nodestack.ha) in msgs

    cli.looper.run(eventually(chk, retryWait=1, timeout=2))


def checkAllNodesStarted(cli, *nodeNames):
    for name in nodeNames:
        checkNodeStarted(cli, name)


def checkClientConnected(cli, nodeNames, clientName):
    printedMsgs = set()
    expectedMsgs = {'{} now connected to {}C'.format(clientName, nodeName)
                    for nodeName in nodeNames}
    for out in cli.printeds:
        msg = out.get('msg')
        if '{} now connected to'.format(clientName) in msg:
            printedMsgs.add(msg)

    assert printedMsgs == expectedMsgs


def checkRequest(cli, looper, operation):
    cName = "Joe"
    cli.enterCmd("new client {}".format(cName))
    # Let client connect to the nodes
    cli.looper.run(eventually(checkClientConnected, cli, list(cli.nodes.keys()),
                              cName, retryWait=1, timeout=5))
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


def newCLI(nodeRegsForCLI, looper, tdir, cliClass=TestCli,
           nodeClass=TestNode,
           clientClass=TestClient):
    mockOutput = MockOutput()
    newcli = cliClass(looper=looper,
                      basedirpath=tdir,
                      nodeReg=nodeRegsForCLI.nodeReg,
                      cliNodeReg=nodeRegsForCLI.cliNodeReg,
                      output=mockOutput,
                      debug=True)
    newcli.NodeClass = nodeClass
    newcli.ClientClass = clientClass
    newcli.basedirpath = tdir
    return newcli


def checkCmdValid(cli, cmd):
    cli.enterCmd(cmd)
    assert 'Invalid command' not in cli.lastCmdOutput


def newKeyPair(cli: TestCli, alias: str=None):
    cmd = "new key {}".format(alias) if alias else "new key"
    keys = 0
    if cli.activeWallet:
        keys = len(cli.activeWallet.signers)
    checkCmdValid(cli, cmd)
    assert len(cli.activeWallet.signers) == keys + 1
    pubKeyMsg = next(s for s in cli.lastCmdOutput.split("\n")
                     if "Identifier for key" in s)
    pubKey = lastWord(pubKeyMsg)
    expected = ['New wallet Default created',
                'Active wallet set to "Default"',
                'Key created in wallet Default',
                'Identifier for key is {}'.format(pubKey),
                'Current identifier set to {}'.format(pubKey)]
    assert cli.lastCmdOutput == "\n".join(expected)

    # the public key and alias are listed
    cli.enterCmd("list ids")
    assert cli.lastMsg().split("\n")[0] == alias if alias else pubKey
    return pubKey


def assertIncremented(f, var):
    before = len(var)
    f()
    after = len(var)
    assert after - before == 1


def lastWord(sentence):
    return sentence.split(" ")[-1]


def assertAllNodesCreated(cli, validNodeNames):
    # Check if all nodes are connected
    checkPoolReady(cli.looper, cli.nodes.values())

    # Check if all nodes are added
    assert len(cli.nodes) == len(validNodeNames)
    assert set(cli.nodes.keys()) == set(cli.nodeReg.keys())
