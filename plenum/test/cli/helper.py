import ast
import os
import stat
import re
import traceback
from tempfile import gettempdir, mkdtemp

import time


import plenum.cli.cli as cli
from plenum.client.wallet import Wallet
from plenum.common.constants import PRIMARY_SELECTION_PREFIX, CURRENT_PROTOCOL_VERSION
from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.common.util import Singleton
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.util import getMaxFailures
from plenum.test.cli.mock_output import MockOutput
from plenum.test.cli.test_keyring import createNewKeyring
from plenum.test.helper import waitForSufficientRepliesForRequests
from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_client import TestClient
from plenum.test.test_node import TestNode, checkPoolReady
from plenum.test.testable import spyable
from pygments.token import Token
from functools import partial
from plenum.test import waits
from plenum.common import util
from plenum.common.request import Request

logger = getlogger()


class Recorder:
    """
    This class will write an interleaved log of the CLI session into a temp
    directory. The directory will start with "cli_scripts_ and should contain
    files for each CLI that was created, e.g., earl, pool, etc.
    """

    def __init__(self, partition):
        basedir = os.path.join(gettempdir(), 'cli_scripts')
        try:
            os.mkdir(basedir)
        except FileExistsError:
            pass
        self.directory = mkdtemp(
            dir=basedir, prefix=time.strftime("%Y%m%d-%H%M%S-"))
        self.filename = os.path.join(self.directory, partition)

    def write(self, data, newline=False):
        with open(self.filename, 'a') as f:
            f.write(data)
            if newline:
                f.write("\n")

    def write_cmd(self, cmd, partition):
        self.write("{}> ".format(partition))
        self.write(cmd, newline=True)


class CombinedRecorder(Recorder, metaclass=Singleton):
    def __init__(self):
        super().__init__('combined')


class TestCliCore:
    def __init__(self):
        self.recorder = None

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
        printeds = [x['msg'] for x in reversed(
            self.printeds[: (len(self.printeds) - self.lastPrintIndex)])]
        printedTokens = [token[1] for tokens in
                         reversed(self.printedTokens[:(
                             len(self.printedTokens) - self.lastPrintedTokenIndex)])
                         for token in tokens.get('tokens', []) if len(token) > 1]
        pt = ''.join(printedTokens)
        return '\n'.join(printeds + [pt]).strip()

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
        logger.debug('CLI got command: {}'.format(cmd))
        self.lastPrintIndex = len(self.printeds)
        self.lastPrintedTokenIndex = len(self.printedTokens)
        if self.recorder:
            self.recorder.write_cmd(cmd, self.unique_name)
        self.parse(cmd)

    def lastMsg(self):
        return self.lastPrintArgs['msg']


@spyable(methods=[cli.Cli.print, cli.Cli.printTokens])
class TestCli(cli.Cli, TestCliCore):
    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     new = logging.StreamHandler(sys.stdout)
    #     self._setHandler('std', new)
    pass


def isErrorToken(token: Token):
    return token == Token.Error


def isHeadingToken(token: Token):
    return token == Token.Heading


def isNameToken(token: Token):
    return token == Token.Name


def waitNodeStarted(cli, nodeName):
    # Node name should be in cli.nodes
    assert nodeName in cli.nodes

    def chk():
        msgs = {stmt['msg'] for stmt in cli.printeds}
        print("checking for {}".format(nodeName))
        print(msgs)
        assert "{} added replica {}:0 to instance 0 (master)" \
            .format(nodeName, nodeName) in msgs
        assert "{} added replica {}:1 to instance 1 (backup)" \
            .format(nodeName, nodeName) in msgs
        assert "{}{} listening for other nodes at {}:{}" \
            .format(CONNECTION_PREFIX, nodeName,
                    *cli.nodes[nodeName].nodestack.ha) \
               in msgs

    startUpTimeout = waits.expectedNodeStartUpTimeout()
    cli.looper.run(eventually(chk, timeout=startUpTimeout))


def waitAllNodesStarted(cli, *nodeNames):
    for name in nodeNames:
        waitNodeStarted(cli, name)


def checkAllNodesUp(cli):
    # TODO: can assertAllNodesCreated be used instead?
    # TODO: can waitAllNodesStarted be used instead?

    msgs = {stmt['msg'] for stmt in cli.printeds}
    expected = PRIMARY_SELECTION_PREFIX + \
        "{nm}:{inst} selected primary {pri}" " for instance {inst} (view 0)"
    assert len(cli.nodes) > 0
    for nm, node in cli.nodes.items():
        assert node
        for inst in [0, 1]:
            rep = node.replicas[inst]
            assert rep
            pri = rep.primaryName
            assert expected.format(nm=nm, pri=pri, inst=inst) in msgs


def waitAllNodesUp(cli):
    timeout = waits.expectedPoolStartUpTimeout(len(cli.nodes))
    cli.looper.run(eventually(checkAllNodesUp, cli, timeout=timeout))


def checkClientConnected(cli, nodeNames, clientName):
    """
    Checks whether client connected to nodes.

    If you do not know the moment when it exactly happens consider using
    'waitClientConnected' instead
    """

    printedMsgs = set()
    stackName = cli.clients[clientName].stackName
    expectedMsgs = {'{}{} now connected to {}C'.format(CONNECTION_PREFIX,
                                                       stackName, nodeName)
                    for nodeName in nodeNames}
    for out in cli.printeds:
        msg = out.get('msg')
        if '{}{} now connected to'.format(CONNECTION_PREFIX, stackName) in msg:
            printedMsgs.add(msg)

    assert printedMsgs == expectedMsgs


def waitClientConnected(cli, nodeNames, clientName):
    """
    Wait for moment when client connected to pool
    """

    timeout = waits.expectedClientToPoolConnectionTimeout(len(nodeNames))
    cli.looper.run(eventually(checkClientConnected, cli,
                              nodeNames, clientName,
                              timeout=timeout))


def checkActiveIdrPrinted(cli):
    assert 'Identifier:' in cli.lastCmdOutput
    assert 'Verification key:' in cli.lastCmdOutput


def createClientAndConnect(cli, nodeNames, clientName):
    cli.enterCmd("new client {}".format(clientName))
    createNewKeyring(clientName, cli)
    cli.enterCmd("new key clientName{}".format("key"))

    waitClientConnected(cli, nodeNames, clientName)


def checkRequest(cli, operation):
    cName = "Joe"
    cli.enterCmd("new client {}".format(cName))
    # Let client connect to the nodes

    nodeNames = list(cli.nodes.keys())
    waitClientConnected(cli, nodeNames, cName)

    # Send request to all nodes

    createNewKeyring(cName, cli)

    cli.enterCmd("new key {}".format("testkey1"))
    assert 'Key created in wallet {}'.format(cName) in cli.lastCmdOutput

    cli.enterCmd('client {} send {}'.format(cName, operation))
    client = cli.clients[cName]
    wallet = cli.wallets[cName]  # type: Wallet
    # Ensure client gets back the replies
    lastReqId = client.reqRepStore.lastReqId

    request = Request(identifier=wallet.defaultId,
                      reqId=lastReqId,
                      protocolVersion=CURRENT_PROTOCOL_VERSION)

    waitForSufficientRepliesForRequests(cli.looper, client,
                                        requests=[request])

    txn, status = client.getReply(wallet.defaultId, lastReqId)

    # Ensure the cli shows appropriate output
    cli.enterCmd('client {} show {}'.format(cName, lastReqId))
    printeds = cli.printeds
    printedReply = printeds[1]
    printedStatus = printeds[0]
    # txnTimePattern = "'txnTime', \d+\.*\d*"
    # txnIdPattern = "'txnId', '" + txn['txnId'] + "'"
    txnTimePattern = "\'txnTime\': \d+\.*\d*"
    # DEPR
    # txnIdPattern = "\'txnId\': '" + txn['txnId'] + "'"
    # assert re.search(txnIdPattern, printedReply['msg'])
    assert re.search(txnTimePattern, printedReply['msg'])
    assert printedStatus['msg'] == "Status: {}".format(status)
    return client, wallet


def newCLI(looper, basedir, ledger_base_dir,
           cliClass=TestCli,
           nodeClass=TestNode,
           clientClass=TestClient,
           config=None,
           partition: str=None,
           unique_name=None,
           logFileName=None,
           name=None,
           agentCreator=None,
           nodes_chroot: str=None):
    if partition:
        recorder = Recorder(partition)
    else:
        recorder = CombinedRecorder()
    mockOutput = MockOutput(recorder=recorder)
    recorder.write("~ be {}\n".format(unique_name))
    otags = config.log_override_tags['cli'] if config else None
    cliClassParams = {
        'looper': looper,
        'basedirpath': basedir,
        'ledger_base_dir': ledger_base_dir,
        'nodeReg': None,
        'cliNodeReg': None,
        'output': mockOutput,
        'debug': True,
        'config': config,
        'unique_name': unique_name,
        'override_tags': otags,
        'logFileName': logFileName,
        'nodes_chroot': nodes_chroot
    }
    if name is not None and agentCreator is not None:
        cliClassParams['name'] = name
        cliClassParams['agentCreator'] = agentCreator

    newcli = cliClass(**cliClassParams)
    newcli.recorder = recorder
    newcli.NodeClass = nodeClass
    newcli.ClientClass = clientClass
    newcli.basedirpath = basedir
    return newcli


def checkCmdValid(cli, cmd):
    cli.enterCmd(cmd)
    assert 'Invalid command' not in cli.lastCmdOutput


def newKeyPair(cli: TestCli, alias: str=None):
    cmd = "new key {}".format(alias) if alias else "new key"
    idrs = set()
    if cli.activeWallet:
        idrs = set(cli.activeWallet.idsToSigners.keys())
    checkCmdValid(cli, cmd)
    assert len(cli.activeWallet.idsToSigners.keys()) == len(idrs) + 1
    new_identifer = set(cli.activeWallet.idsToSigners.keys()
                        ).difference(idrs).pop()
    expected = ['Key created in wallet Default']
    if alias:
        idr = cli.activeWallet.aliasesToIds.get(alias)
        verkey = cli.activeWallet.getVerkey(idr)
        expected.append('DID for key is {}'.
                        format(idr))
        expected.append('Verification key is {}'.format(verkey))
        expected.append('Alias for DID is {}'.format(alias))
    else:
        expected.append('DID for key is {}'.format(new_identifer))
        verkey = cli.activeWallet.getVerkey(new_identifer)
        expected.append('Verification key is {}'.format(verkey))

    expected.append('Current DID set to {}'.format(alias or new_identifer))

    # TODO: Reconsider this
    # Using `in` rather than `=` so as to take care of the fact that this might
    # be the first time wallet is accessed so wallet would be created and some
    # output corresponding to that would be printed.
    assert "\n".join(expected) in cli.lastCmdOutput

    # the public key and alias are listed
    cli.enterCmd("list ids")
    needle = alias if alias else new_identifer
    # assert cli.lastMsg().split("\n")[0] == alias if alias else pubKey
    assert needle in cli.lastCmdOutput
    return new_identifer


pluginLoadedPat = re.compile(
    "plugin [A-Za-z0-9_]+ successfully loaded from module")


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
    assert cli.lastCmdOutput == "No such client. See: 'help new client' for " \
                                "more details"


# replyPat = re.compile("C: odict\((.+)\)$")
replyPat = re.compile("C: ({.+$)")


# def checkReply(cli, count, clbk):
#     done = 0
#     for out in cli.printeds:
#         msg = out['msg']
#         m = replyPat.search(msg)
#         if m:
#             if clbk(m.groups(0)[0].strip()):
#             # result = ast.literal_eval(m.groups(0)[0].strip())
#             # if clbk(result):
#                 done += 1
#     assert done == count
#
#
# def checkSuccess(data):
#     return data and "('success', True)" in data
#
#
# balancePat = re.compile("\('balance', (\d+)\)")
#
#
# def checkBalance(balance, data):
#     if checkSuccess(data):
#         searched = balancePat.search(data)
#         if searched:
#             return int(searched.group(1)) == balance

def checkReply(cli, count, clbk):
    done = 0
    for out in cli.printeds:
        msg = out['msg']
        m = replyPat.search(msg)
        if m:
            result = ast.literal_eval(m.groups(0)[0].strip())
            if clbk(result):
                done += 1
    logger.warning("Done = {}, Count = {}".format(done, count))
    assert done == count


def checkSuccess(data):
    result = data.get('result')
    return result and result.get('success')


def checkBalance(balance, data):
    if checkSuccess(data):
        result = data.get('result')
        return result.get('balance') == balance


def waitForReply(cli, nodeCount, replyChecker, customTimeout=None):
    timeout = customTimeout or \
        waits.expectedTransactionExecutionTime(nodeCount)
    cli.looper.run(eventually(checkReply, cli,
                              nodeCount, replyChecker,
                              timeout=timeout))


def waitRequestSuccess(cli, nodeCount, customTimeout=None):
    waitForReply(cli, nodeCount, checkSuccess, customTimeout)


def waitBalanceChange(cli, nodeCount, balanceValue, customTimeout=None):
    waitForReply(cli, nodeCount,
                 partial(checkBalance, balanceValue), customTimeout)


def loadPlugin(cli, pluginPkgName):
    curPath = os.path.dirname(os.path.dirname(__file__))
    fullPath = os.path.join(curPath, 'plugin', pluginPkgName)
    cli.enterCmd("load plugins from {}".format(fullPath))
    m = pluginLoadedPat.search(cli.printeds[0]['msg'])
    assert m


def assertCliTokens(matchedVars, tokens):
    for key, expectedValue in tokens.items():
        matchedValue = matchedVars.get(key)

        if expectedValue is not None:
            assert matchedValue is not None, \
                "Key '{}' not found in machedVars (matchedValue={})".\
                format(key, matchedValue)

        expectedValueLen = len(expectedValue) if expectedValue else 0
        matchedValueLen = len(matchedValue) if matchedValue else 0

        assert matchedValue == expectedValue, \
            "Value not matched for key '{}', " \
            "\nexpectedValue (length: {}): {}, " \
            "\nactualValue (length: {}): {}".\
            format(key, expectedValueLen, expectedValue,
                   matchedValueLen, matchedValue)


def doByCtx(ctx):
    def _(attempt, expect=None, within=None, mapper=None, not_expect=None):
        assert expect is not None or within is None, \
            "'within' not applicable without 'expect'"
        cli = ctx['current_cli']

        # This if was not there earlier, but I felt a need to reuse this
        # feature (be, do, expect ...) without attempting anything
        # mostly because there will be something async which will do something,
        # hence I added the below if check

        if attempt:
            attempt = attempt.format(**mapper) if mapper else attempt
            cli.enterCmd(attempt)

        def getAssertErrorMsg(e, cli, exp: bool, actual: bool):
            length = 80
            sepLines = "\n" + "*" * length + "\n" + "-" * length
            commonMsg = "\n{}\n{}".format(
                cli.lastCmdOutput, sepLines)
            prefix = ""
            if exp and not actual:
                prefix = "NOT found "
            elif not exp and actual:
                prefix = "FOUND "
            return "{}\n{}\n\n{} in\n {}".format(
                sepLines, e, prefix, commonMsg)

        def check():
            nonlocal expect
            nonlocal not_expect

            def chk(obj, parity=True):
                if not obj:
                    return
                if isinstance(obj, str) or callable(obj):
                    obj = [obj]
                for e in obj:
                    if isinstance(e, str):
                        e = e.format(**mapper) if mapper else e
                        try:

                            if parity:
                                assert e in cli.lastCmdOutput, \
                                    getAssertErrorMsg(
                                        e, cli, exp=True, actual=False)
                            else:
                                assert e not in cli.lastCmdOutput, \
                                    getAssertErrorMsg(
                                        e, cli, exp=False, actual=True)
                        except AssertionError as e:
                            extraMsg = ""
                            if not within:
                                extraMsg = "NOTE: 'within' parameter was not " \
                                           "provided, if test should wait for" \
                                           " sometime before considering this" \
                                           " check failed, then provide that" \
                                           " parameter with appropriate value"
                                separator = "-" * len(extraMsg)
                                extraMsg = "\n\n{}\n{}\n{}".format(separator,
                                                                   extraMsg,
                                                                   separator)
                            raise (AssertionError("{}{}".format(e, extraMsg)))
                    elif callable(e):
                        # callables should raise exceptions to signal an error
                        if parity:
                            e(cli)
                        else:
                            try:
                                e(cli)
                            except BaseException:
                                # Since its a test so not using logger is not
                                # a big deal
                                traceback.print_exc()
                                continue
                            raise RuntimeError("did not expect success")
                    else:
                        raise AttributeError("only str, callable, or "
                                             "collections of str and callable "
                                             "are allowed")
            chk(expect)
            chk(not_expect, False)
        if within:
            cli.looper.run(eventually(check, timeout=within))
        else:
            check()
    return _


def checkWalletFilePersisted(filePath):
    assert os.path.exists(filePath)


def checkPermissions(path, mode):
    assert stat.S_IMODE(os.stat(path).st_mode) == mode


def checkWalletRestored(cli, expectedWalletKeyName,
                        expectedIdentifiers):

    cli.lastCmdOutput == "Saved wallet {} restored".format(
        expectedWalletKeyName)
    assert cli._activeWallet.name == expectedWalletKeyName
    assert len(cli._activeWallet.identifiers) == \
        expectedIdentifiers


def getOldIdentifiersForActiveWallet(cli):
    oldIdentifiers = 0
    if cli._activeWallet:
        oldIdentifiers = len(cli._activeWallet.identifiers)
    return oldIdentifiers


def createAndAssertNewCreation(do, cli, keyringName):
    oldIdentifiers = getOldIdentifiersForActiveWallet(cli)
    do('new key', within=2,
       expect=["Key created in wallet {}".format(keyringName)])
    assert len(cli._activeWallet.identifiers) == oldIdentifiers + 1


def createAndAssertNewKeyringCreation(do, name, expectedMsgs=None):
    finalExpectedMsgs = expectedMsgs if expectedMsgs else [
        'Active wallet set to "{}"'.format(name),
        'New wallet {} created'.format(name)
    ]
    do('new wallet {}'.format(name), expect=finalExpectedMsgs)


def useAndAssertKeyring(do, name, expectedName=None, expectedMsgs=None):
    keyringName = expectedName or name
    finalExpectedMsgs = expectedMsgs or \
        ['Active wallet set to "{}"'.format(keyringName)]
    do('use wallet {}'.format(name),
       expect=finalExpectedMsgs
       )


def saveAndAssertKeyring(do, name, expectedName=None, expectedMsgs=None):
    keyringName = expectedName or name
    finalExpectedMsgs = expectedMsgs or \
        ['Active wallet "{}" saved'.format(keyringName)]
    do('save wallet'.format(name),
       expect=finalExpectedMsgs
       )


def exitFromCli(do):
    import pytest
    with pytest.raises(cli.Exit):
        do('exit', expect='Goodbye.')


def restartCliAndAssert(cli, do, expectedRestoredWalletName,
                        expectedIdentifiers):
    do(None, expect=[
        'Saved wallet "{}" restored'.format(expectedRestoredWalletName),
        'Active wallet set to "{}"'.format(expectedRestoredWalletName)
    ], within=5)
    assert cli._activeWallet is not None
    assert len(cli._activeWallet.identifiers) == expectedIdentifiers
