import base64
from copy import copy

import pytest
from raet.nacling import Privateer

from plenum.client.signer import SimpleSigner
from plenum.common.looper import Looper
from plenum.common.raet import initLocalKeep
from plenum.common.txn import NEW_CLIENT
from plenum.common.types import CLIENT_STACK_SUFFIX
from plenum.common.util import getlogger, getMaxFailures, \
    randomString
from plenum.test.eventually import eventually
from plenum.test.helper import TestNode, TestClient, genHa, \
    checkNodesConnected, sendReqsToNodesAndVerifySuffReplies, \
    checkProtocolInstanceSetup
from plenum.test.node_catchup.helper import checkNodeLedgersForEqualSize
from plenum.test.pool_transactions.helper import addNewClient, addNewNode, \
    changeNodeIp, addNewStewardAndNode, changeNodeKeys

logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


@pytest.fixture(scope="module")
def steward1(looper, txnPoolNodeSet, poolTxnStewardData, txnPoolCliNodeReg,
             tdirWithPoolTxns):
    name, pkseed, sigseed = poolTxnStewardData
    signer = SimpleSigner(seed=sigseed)
    steward = TestClient(name=name, nodeReg=txnPoolCliNodeReg, ha=genHa(),
                         signer=signer, basedirpath=tdirWithPoolTxns)
    looper.add(steward)
    looper.run(steward.ensureConnectedToNodes())
    return steward


@pytest.fixture(scope="module")
def client1(txnPoolNodeSet, poolTxnClientData, txnPoolCliNodeReg,
             tdirWithPoolTxns):
    name, pkseed, sigseed = poolTxnClientData
    signer = SimpleSigner(seed=sigseed)
    client = TestClient(name=name, nodeReg=txnPoolCliNodeReg, ha=genHa(),
                         signer=signer, basedirpath=tdirWithPoolTxns)
    return client


def testNodesConnect(txnPoolNodeSet):
    pass


def testNodesReceiveClientMsgs(looper, client1):
    looper.add(client1)
    looper.run(client1.ensureConnectedToNodes())
    sendReqsToNodesAndVerifySuffReplies(looper, client1, 1)


def testAddNewClient(looper, txnPoolNodeSet, steward1):
    newSigner = addNewClient(NEW_CLIENT, looper, steward1, randomString())

    def chk():
        for node in txnPoolNodeSet:
            assert newSigner.verstr in node.clientAuthNr.clients

    looper.run(eventually(chk, retryWait=1, timeout=5))


def testStewardCannotAddMoreThanOneNode(looper, txnPoolNodeSet, steward1,
                                   txnPoolCliNodeReg, tconf):
    newNodeName = "Epsilon"
    with pytest.raises(AssertionError):
        addNewNode(looper, steward1, newNodeName, txnPoolCliNodeReg, tconf)


def testClientConnectsToNewNode(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                txnPoolCliNodeReg, tconf, steward1):
    """
    A client should be able to connect to a newly added node
    """
    newStewardName = "testClientSteward"+randomString(3)
    newNodeName = "Epsilon"
    oldNodeReg = copy(steward1.nodeReg)
    newSteward, newNode = addNewStewardAndNode(looper, steward1, newStewardName,
                                               newNodeName, txnPoolCliNodeReg,
                                               tdirWithPoolTxns, tconf)
    txnPoolNodeSet.append(newNode)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    logger.debug("{} connected to the pool".format(newNode))

    def chkNodeRegRecvd():
        assert (len(steward1.nodeReg) - len(oldNodeReg)) == 1
        assert (newNode.name + CLIENT_STACK_SUFFIX) in steward1.nodeReg

    looper.run(eventually(chkNodeRegRecvd, retryWait=1, timeout=5))
    looper.run(steward1.ensureConnectedToNodes())
    looper.run(newSteward.ensureConnectedToNodes())


def testAdd2NewNodes(looper, txnPoolNodeSet, tdirWithPoolTxns,
                     txnPoolCliNodeReg, tconf, steward1):
    """
    Add 2 new nodes to trigger replica addition and primary election
    """

    for nodeName in ("Zeta", "Eta"):
        newStewardName = "testClientSteward"+randomString(3)
        newSteward, newNode = addNewStewardAndNode(looper, steward1,
                                                   newStewardName,
                                                   nodeName,
                                                   txnPoolCliNodeReg,
                                                   tdirWithPoolTxns, tconf)
        txnPoolNodeSet.append(newNode)
        looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                              timeout=5))
        logger.debug("{} connected to the pool".format(newNode))
        looper.run(eventually(checkNodeLedgersForEqualSize, newNode,
                              *txnPoolNodeSet[:4], retryWait=1, timeout=5))

    def checkFValue():
        for node in txnPoolNodeSet:
            f = getMaxFailures(len(txnPoolNodeSet))
            assert node.f == f
            assert len(node.replicas) == (f + 1)

    looper.run(eventually(checkFValue, retryWait=1, timeout=5))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1,
                               timeout=5)


@pytest.fixture("module")
def nodeThetaAdded(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        txnPoolCliNodeReg, tconf, steward1):
    newStewardName = "testClientSteward" + randomString(3)
    newNodeName = "Theta"
    newSteward, newNode = addNewStewardAndNode(looper, steward1, newStewardName,
                                               newNodeName, txnPoolCliNodeReg,
                                               tdirWithPoolTxns, tconf)
    txnPoolNodeSet.append(newNode)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    looper.run(steward1.ensureConnectedToNodes())
    looper.run(newSteward.ensureConnectedToNodes())
    return newSteward, newNode


def testNodePortChanged(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        tconf, steward1, nodeThetaAdded):
    """
    An running node's port is changed
    """
    newSteward, newNode = nodeThetaAdded
    newNode.stop()
    nodeNewHa, clientNewHa = genHa(2)
    changeNodeIp(looper, newSteward,
                 newNode, nodeHa=nodeNewHa, clientHa=clientNewHa,
                 baseDir=tdirWithPoolTxns, conf=tconf)
    looper.removeProdable(newNode)
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeNewHa, cliha=clientNewHa)
    looper.add(node)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    looper.run(steward1.ensureConnectedToNodes())
    looper.run(newSteward.ensureConnectedToNodes())


def testNodeKeysChanged(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        tconf, steward1, nodeThetaAdded):
    newSteward, newNode = nodeThetaAdded
    newNode.stop()
    nodeHa, nodeCHa = newNode.nodestack.ha, newNode.clientstack.ha
    sigseed = randomString(32).encode()
    pkseed = randomString(32).encode()
    pubkey = Privateer(pkseed).pubhex.decode()
    verkey = SimpleSigner(seed=sigseed).verkey.decode()
    changeNodeKeys(looper, newSteward, newNode, verkey, pubkey,
                   tdirWithPoolTxns, tconf)
    initLocalKeep(newNode.name, tdirWithPoolTxns, pkseed, sigseed)
    initLocalKeep(newNode.name+CLIENT_STACK_SUFFIX, tdirWithPoolTxns, pkseed,
                  sigseed)
    looper.removeProdable(newNode)
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeHa, cliha=nodeCHa)
    looper.add(node)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    looper.run(steward1.ensureConnectedToNodes())
    looper.run(newSteward.ensureConnectedToNodes())

