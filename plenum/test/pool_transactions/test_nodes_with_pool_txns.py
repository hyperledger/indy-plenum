from copy import copy

import pytest

from plenum.common.eventually import eventually
from plenum.common.log import getlogger
from plenum.common.port_dispenser import genHa
from plenum.common.raet import initLocalKeep
from plenum.common.signer_simple import SimpleSigner
from plenum.common.types import CLIENT_STACK_SUFFIX, HA
from plenum.common.util import getMaxFailures, randomString
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    checkReqNackWithReason
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewClient, addNewNode, \
    changeNodeHa, addNewStewardAndNode, changeNodeKeys
from plenum.test.test_node import TestNode, checkNodesConnected, \
    checkProtocolInstanceSetup

logger = getlogger()

# logged errors to ignore
whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message',
             'got error while verifying message']
# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it


def getNodeWithName(txnPoolNodeSet, name: str):
    return next(node for node in txnPoolNodeSet if node.name == name)


def testNodesConnect(txnPoolNodeSet):
    pass


def testNodesReceiveClientMsgs(looper, txnPoolNodeSet, wallet1, client1,
                               client1Connected):
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client1,
                                                  *txnPoolNodeSet)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)


def testAddNewClient(looper, txnPoolNodeSet, steward1, stewardWallet):
    wallet = addNewClient(None, looper, steward1, stewardWallet, randomString())

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


def testNonStewardCannotAddNode(looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected, tdirWithPoolTxns,
                                tconf, allPluginsPath):
    newNodeName = "Epsilon"
    with pytest.raises(AssertionError):
        addNewNode(looper, client1, wallet1, newNodeName,
                   tdirWithPoolTxns, tconf, allPluginsPath)

    for node in txnPoolNodeSet:
        checkReqNackWithReason(client1, 'is not a steward so cannot add a '
                                        'new node', node.clientstack.name)


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
    looper.run(checkNodesConnected(txnPoolNodeSet))
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
        looper.run(checkNodesConnected(txnPoolNodeSet))
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


def testNodePortCannotBeChangedByAnotherSteward(looper, txnPoolNodeSet,
                                                tdirWithPoolTxns, tconf,
                                                steward1, stewardWallet,
                                                nodeThetaAdded):
    _, _, newNode = nodeThetaAdded
    nodeNewHa, clientNewHa = genHa(2)
    logger.debug('{} changing HAs to {} {}'.format(newNode, nodeNewHa,
                                                   clientNewHa))
    with pytest.raises(AssertionError):
        changeNodeHa(looper, steward1, stewardWallet, newNode,
                     nodeHa=nodeNewHa, clientHa=clientNewHa)

    for node in txnPoolNodeSet:
        checkReqNackWithReason(steward1, 'is not a steward of node',
                               node.clientstack.name)


def testNodePortChanged(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        tconf, steward1, stewardWallet, nodeThetaAdded):
    """
    An running node's port is changed
    """
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    nodeNewHa, clientNewHa = genHa(2)
    logger.debug("{} changing HAs to {} {}".format(newNode, nodeNewHa,
                                                   clientNewHa))
    changeNodeHa(looper, newSteward, newStewardWallet, newNode,
                 nodeHa=nodeNewHa, clientHa=clientNewHa)
    newNode.stop()
    looper.removeProdable(name=newNode.name)
    logger.debug("{} starting with HAs {} {}".format(newNode, nodeNewHa,
                                                     clientNewHa))
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeNewHa, cliha=clientNewHa)
    looper.add(node)
    # The last element of `txnPoolNodeSet` is the node Theta that was just
    # stopped
    txnPoolNodeSet[-1] = node
    looper.run(checkNodesConnected(txnPoolNodeSet))
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
    verkey = SimpleSigner(seed=sigseed).naclSigner.verhex.decode()
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
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.run(eventually(checkNodeLedgersForEquality, node,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=10))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)
