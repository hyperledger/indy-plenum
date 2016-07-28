import ast
import json
import os
import re

from pygments.token import Token

import plenum.cli.cli as cli
from plenum.common.util import getMaxFailures
from plenum.test.cli.mock_output import MockOutput
from plenum.test.eventually import eventually
from plenum.test.testable import Spyable
from plenum.test.helper import getAllArgs, checkSufficientRepliesRecvd, \
    TestNode, checkPoolReady, TestClient


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
        #TODO Dont reverese self.printeds, just take (len(self.printeds) -
        # self.lastPrintIndex) number of entries from beginning of
        printeds = [x['msg'] for x in list(reversed(self.printeds))[self.lastPrintIndex:]]
        printedTokens = [token[1] for tokens in
                         list(reversed(self.printedTokens))[self.lastPrintedTokenIndex:]
                         for token in tokens.get('tokens', []) if len(token) > 1]
        return ''.join(printeds + [' '] + printedTokens).strip()

    # noinspection PyAttributeOutsideInit
    @property
    def lastPrintIndex(self):
        if not hasattr(self, "_lastPrintIndex"):
            self._lastPrintIndex = 0
        return self._lastPrintIndex

    # noinspection PyAttributeOutsideInit
    @lastPrintIndex.setter
    def lastPrintIndex(self, index: int) -> None:
        self._lastPrintIndex = index

    # noinspection PyAttributeOutsideInit
    @property
    def lastPrintedTokenIndex(self):
        if not hasattr(self, "_lastPrintedTokenIndex"):
            self._lastPrintedTokenIndex = 0
        return self._lastPrintedTokenIndex

    # noinspection PyAttributeOutsideInit
    @lastPrintedTokenIndex.setter
    def lastPrintedTokenIndex(self, index: int) -> None:
        self._lastPrintedTokenIndex = index

    # noinspection PyUnresolvedReferences
    def enterCmd(self, cmd: str):
        self.lastPrintIndex = len(self.printeds)
        self.lastPrintedTokenIndex = len(self.printedTokens)
        self.parse(cmd)

    def lastMsg(self):
        return self.lastPrintArgs['msg']


@Spyable(methods=[cli.Cli.print, cli.Cli.printTokens])
class TestCli(cli.Cli, TestCliCore):
    pass


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


def checkAllNodesUp(cli):
    msgs = {stmt['msg'] for stmt in cli.printeds}
    expected = "{nm}:{inst} selected primary {pri} " \
               "for instance {inst} (view 0)"
    for nm, node in cli.nodes.items():
        assert node
        for inst in [0, 1]:
            rep = node.replicas[inst]
            assert rep
            pri = rep.primaryNames[0]
            assert expected.format(nm=nm, pri=pri, inst=inst) in msgs


def checkClientConnected(cli, nodeNames, clientName):
    printedMsgs = set()
    expectedMsgs = {'{} now connected to {}C'.format(clientName, nodeName)
                    for nodeName in nodeNames}
    for out in cli.printeds:
        msg = out.get('msg')
        if '{} now connected to'.format(clientName) in msg:
            printedMsgs.add(msg)

    assert printedMsgs == expectedMsgs


def createClientAndConnect(cli, nodeNames, clientName):
    cli.enterCmd("new client {}".format(clientName))
    cli.looper.run(eventually(checkClientConnected, cli, nodeNames,
                              clientName, retryWait=1, timeout=3))


def checkRequest(cli, operation):
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
    cli.looper.run(eventually(
            checkSufficientRepliesRecvd,
            client.inBox,
            client.lastReqId,
            f,
            retryWait=2,
            timeout=10))

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
    keys = set()
    if cli.activeWallet:
        keys = set(cli.activeWallet.signers.keys())
    checkCmdValid(cli, cmd)
    assert len(cli.activeWallet.signers.keys()) == len(keys) + 1
    pubKey = set(cli.activeWallet.signers.keys()).difference(keys).pop()
    expected = ['Key created in wallet Default',
                'Identifier for key is {}'.format(pubKey),
                'Current identifier set to {}'.format(pubKey)]
    # TODO: Reconsider this
    # Using `in` rather than `=` so as to take care of the fact that this might
    # be the first time wallet is accessed so wallet would be created and some
    # output corresponding to that would be printed.
    assert "".join(expected) in cli.lastCmdOutput

    # the public key and alias are listed
    cli.enterCmd("list ids")
    needle = alias if alias else pubKey
    # assert cli.lastMsg().split("\n")[0] == alias if alias else pubKey
    assert needle in cli.lastCmdOutput
    return pubKey


replyPat = re.compile("C: ({.+$)")
pluginLoadedPat = re.compile("plugin [A-Za-z0-9_]+ loaded from module")


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


def assertNoClient(cli):
    assert cli.lastCmdOutput == "No such client. See: 'help new' for " \
                                "more details"


def checkReply(cli, count, clbk):
    done = 0
    for out in cli.printeds:
        msg = out['msg']
        m = replyPat.search(msg)
        if m:
            result = ast.literal_eval(m.groups(0)[0].strip())
            if clbk(result):
                done += 1
    assert done == count


def checkSuccess(data):
    result = data.get('result')
    return result and result.get('success') == True


def checkBalance(balance, data):
    if checkSuccess(data):
        result = data.get('result')
        return result.get('balance') == balance


def loadPlugin(cli, pluginPkgName):
    curPath = os.path.dirname(os.path.dirname(__file__))
    fullPath = os.path.join(curPath, 'plugin', pluginPkgName)
    cli.enterCmd("load plugins from {}".format(fullPath))
    m = pluginLoadedPat.search(cli.printeds[0]['msg'])
    assert m
