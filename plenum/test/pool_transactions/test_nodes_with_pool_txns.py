from copy import copy

import pytest

from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.raet import initLocalKeep
from plenum.common.txn import USER
from plenum.common.types import CLIENT_STACK_SUFFIX, HA
from plenum.common.util import getlogger, getMaxFailures, \
    randomString
from plenum.test.eventually import eventually
from plenum.test.helper import TestNode, genHa, \
    checkNodesConnected, sendReqsToNodesAndVerifySuffReplies, \
    checkProtocolInstanceSetup
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewClient, addNewNode, \
    changeNodeHa, addNewStewardAndNode, changeNodeKeys, buildPoolClientAndWallet

logger = getlogger()

# logged errors to ignore
whitelist = ['found legacy entry', "doesn't match", "reconciling nodeReg",
             "missing", "conflicts", "matches", "nodeReg", "conflicting address"]


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


@pytest.fixture(scope="module")
def clientAndWallet1(txnPoolNodeSet, poolTxnClientData, tdirWithPoolTxns):
    # name, sigseed = poolTxnClientData
    # w = Wallet(name)
    # w.addSigner(seed=sigseed)
    # return genTestClient(name=name, identifier=w.defaultId,
    #                      tmpdir=tdirWithPoolTxns, usePoolLedger=True), w
    return buildPoolClientAndWallet(poolTxnClientData, tdirWithPoolTxns)


@pytest.fixture(scope="module")
def client1(clientAndWallet1):
    return clientAndWallet1[0]


@pytest.fixture(scope="module")
def wallet1(clientAndWallet1):
    return clientAndWallet1[1]


@pytest.fixture(scope="module")
def stewardAndWallet1(looper, txnPoolNodeSet, poolTxnStewardData,
                      tdirWithPoolTxns):
    return buildPoolClientAndWallet(poolTxnStewardData, tdirWithPoolTxns)


@pytest.fixture(scope="module")
def steward1(looper, txnPoolNodeSet, stewardAndWallet1):
    # name, sigseed = poolTxnStewardData
    # signer = SimpleSigner(seed=sigseed)
    # steward = TestClient(name=name, nodeReg=None, ha=genHa(),
    #                      signer=signer, basedirpath=tdirWithPoolTxns)
    # steward, wallet = buildPoolClientAndWallet(poolTxnStewardData,
    #                                            tdirWithPoolTxns)
    steward, wallet = stewardAndWallet1
    looper.add(steward)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward,
                                                  *txnPoolNodeSet)
    return steward


@pytest.fixture(scope="module")
def stewardWallet(stewardAndWallet1):
    return stewardAndWallet1[1]


@pytest.fixture("module")
def nodeThetaAdded(looper, txnPoolNodeSet, tdirWithPoolTxns, tconf, steward1,
                   stewardWallet, allPluginsPath):
    newStewardName = "testClientSteward" + randomString(3)
    newNodeName = "Theta"
    newSteward, newStewardWallet, newNode = addNewStewardAndNode(looper,
                                               steward1, stewardWallet,
                                               newStewardName, newNodeName,
                                               tdirWithPoolTxns, tconf,
                                               allPluginsPath)
    txnPoolNodeSet.append(newNode)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)
    return newSteward, newStewardWallet, newNode


@pytest.fixture("module")
def newHa():
    return genHa(2)


def getNodeWithName(txnPoolNodeSet, name: str):
    return next(node for node in txnPoolNodeSet if node.name == name)


def testNodesConnect(txnPoolNodeSet):
    pass


def testNodesReceiveClientMsgs(looper, wallet1, client1, txnPoolNodeSet):
    looper.add(client1)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client1,
                                                  *txnPoolNodeSet)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)


def testAddNewClient(looper, txnPoolNodeSet, steward1, stewardWallet):
    wallet = addNewClient(USER, looper, steward1, stewardWallet,
                             randomString())

    def chk():
        for node in txnPoolNodeSet:
            assert wallet.defaultId in node.clientAuthNr.clients

    looper.run(eventually(chk, retryWait=1, timeout=5))


def testStewardCannotAddMoreThanOneNode(looper, txnPoolNodeSet, steward1,
                                        stewardWallet, tdirWithPoolTxns, tconf,
                                        allPluginsPath):
    newNodeName = "Epsilon"
    with pytest.raises(AssertionError):
        addNewNode(looper, steward1, stewardWallet, newNodeName,
                   tdirWithPoolTxns, tconf, allPluginsPath)


def testClientConnectsToNewNode(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                tconf, steward1, stewardWallet, allPluginsPath):
    """
    A client should be able to connect to a newly added node
    """
    newStewardName = "testClientSteward"+randomString(3)
    newNodeName = "Epsilon"
    oldNodeReg = copy(steward1.nodeReg)
    newSteward, newStewardWallet, newNode = addNewStewardAndNode(looper,
                                                steward1, stewardWallet,
                                                newStewardName, newNodeName,
                                                tdirWithPoolTxns, tconf,
                                                allPluginsPath)
    txnPoolNodeSet.append(newNode)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    logger.debug("{} connected to the pool".format(newNode))

    def chkNodeRegRecvd():
        assert (len(steward1.nodeReg) - len(oldNodeReg)) == 1
        assert (newNode.name + CLIENT_STACK_SUFFIX) in steward1.nodeReg

    looper.run(eventually(chkNodeRegRecvd, retryWait=1, timeout=5))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)


def testAdd2NewNodes(looper, txnPoolNodeSet, tdirWithPoolTxns, tconf, steward1,
                     stewardWallet, allPluginsPath):
    """
    Add 2 new nodes to trigger replica addition and primary election
    """
    for nodeName in ("Zeta", "Eta"):
        newStewardName = "testClientSteward"+randomString(3)
        newSteward, newStewardWallet, newNode = addNewStewardAndNode(looper,
                                                   steward1,
                                                   stewardWallet,
                                                   newStewardName,
                                                   nodeName,
                                                   tdirWithPoolTxns, tconf,
                                                   allPluginsPath)
        txnPoolNodeSet.append(newNode)
        looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                              timeout=5))
        logger.debug("{} connected to the pool".format(newNode))
        looper.run(eventually(checkNodeLedgersForEquality, newNode,
                              *txnPoolNodeSet[:-1], retryWait=1, timeout=7))

    f = getMaxFailures(len(txnPoolNodeSet))

    def checkFValue():
        for node in txnPoolNodeSet:
            assert node.f == f
            assert len(node.replicas) == (f + 1)

    looper.run(eventually(checkFValue, retryWait=1, timeout=5))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1,
                               timeout=5)


def testNodePortChanged(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        tconf, steward1, stewardWallet, nodeThetaAdded, newHa):
    """
    An running node's port is changed
    """
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    newNode.stop()
    nodeNewHa, clientNewHa = newHa
    logger.debug("{} changing HAs to {} {}".format(newNode, nodeNewHa,
                                                   clientNewHa))
    changeNodeHa(looper, newSteward, newStewardWallet, newNode,
                 nodeHa=nodeNewHa, clientHa=clientNewHa)
    looper.removeProdable(name=newNode.name)
    logger.debug("{} starting with HAs {} {}".format(newNode, nodeNewHa,
                                                     clientNewHa))
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeNewHa, cliha=clientNewHa)
    looper.add(node)
    # The last element of `txnPoolNodeSet` is the node Theta that was just
    # stopped
    txnPoolNodeSet[-1] = node
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    looper.run(eventually(checkNodeLedgersForEquality, node,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=10))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)


def testNodeKeysChanged(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        tconf, steward1, nodeThetaAdded,
                        allPluginsPath=None):
    newSteward, newStewardWallet, newNode = nodeThetaAdded

    # Since the node returned by fixture `nodeThetaAdded` was abandoned in the
    # previous test, so getting node `Theta` from `txnPoolNodeSet`
    newNode = getNodeWithName(txnPoolNodeSet, newNode.name)

    newNode.stop()
    nodeHa, nodeCHa = HA(*newNode.nodestack.ha), HA(*newNode.clientstack.ha)
    sigseed = randomString(32).encode()
    verkey = SimpleSigner(seed=sigseed).verkey.decode()
    changeNodeKeys(looper, newSteward, newStewardWallet, newNode, verkey)
    initLocalKeep(newNode.name, tdirWithPoolTxns, sigseed)
    initLocalKeep(newNode.name+CLIENT_STACK_SUFFIX, tdirWithPoolTxns, sigseed)
    looper.removeProdable(name=newNode.name)
    logger.debug("{} starting with HAs {} {}".format(newNode, nodeHa, nodeCHa))
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(node)
    # The last element of `txnPoolNodeSet` is the node Theta that was just
    # stopped
    txnPoolNodeSet[-1] = node
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    looper.run(eventually(checkNodeLedgersForEquality, node,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=10))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)
