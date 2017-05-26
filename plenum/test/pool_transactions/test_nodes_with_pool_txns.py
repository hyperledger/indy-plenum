import itertools
from copy import copy

import base58
import pytest

from plenum.common.keygen_utils import initNodeKeysForBothStacks
from stp_core.network.port_dispenser import genHa
from stp_core.types import HA

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.signer_simple import SimpleSigner
from plenum.common.constants import *
from plenum.common.util import getMaxFailures, randomString
from plenum.test import waits
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    checkRejectWithReason, waitReqNackWithReason, waitRejectWithReason, \
    waitForSufficientRepliesForRequests, waitReqNackFromPoolWithReason, \
    waitRejectFromPoolWithReason
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewClient, addNewNode, \
    updateNodeData, addNewStewardAndNode, changeNodeKeys, sendUpdateNode, \
    sendAddNewNode, updateNodeDataAndReconnect, addNewSteward
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

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))


def testStewardCannotAddNodeWithNonBase58VerKey(looper, tdir,
                                                txnPoolNodeSet,
                                                newAdHocSteward):
    """
    The Case:
        Steward accidentally sends the NODE txn with a non base58 verkey.
    The expected result:
        Steward gets NAck response from the pool.
    """
    # create a new steward
    newSteward, newStewardWallet = newAdHocSteward

    newNodeName = "Epsilon"

    # get hex VerKey
    sigseed = randomString(32).encode()
    nodeSigner = SimpleSigner(seed=sigseed)
    b = base58.b58decode(nodeSigner.identifier)
    hexVerKey = bytearray(b).hex()

    def _setHexVerkey(op):
        op[TARGET_NYM] = hexVerKey
        return op

    sendAddNewNode(newNodeName, newSteward, newStewardWallet,
                   transformOpFunc=_setHexVerkey)
    waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                  'is not a base58 string')


def testStewardCannotAddNodeWithInvalidHa(looper, tdir,
                                           txnPoolNodeSet,
                                           newAdHocSteward):
    """
    The case:
        Steward accidentally sends the NODE txn with an invalid HA.
    The expected result:
        Steward gets NAck response from the pool.
    """
    newNodeName = "Epsilon"

    newSteward, newStewardWallet = newAdHocSteward

    # a sequence of the test cases for each field
    tests = itertools.chain(
        itertools.product(
            (NODE_IP, CLIENT_IP), ('127.0.0.1 ', '256.0.0.1', '0.0.0.0')
        ),
        itertools.product(
            (NODE_PORT, CLIENT_PORT), ('foo', '9700', 0, 65535 + 1, 4351683546843518184)
        ),
    )

    for field, value in tests:
        # create a transform function for each test
        def _tnf(op): op[DATA].update({field: value})
        sendAddNewNode(newNodeName, newSteward, newStewardWallet,
                       transformOpFunc=_tnf)
        # wait NAcks with exact message. it does not works for just 'is invalid'
        # because the 'is invalid' will check only first few cases
        waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                      "'{}' ('{}') is invalid".format(field, value))


def testStewardCannotAddNodeWithOutFullFieldsSet(looper, tdir,
                                 txnPoolNodeSet,
                                 newAdHocSteward):
    """
    The case:
        Steward accidentally sends the NODE txn without full fields set.
    The expected result:
        Steward gets NAck response from the pool.
    """
    newNodeName = "Epsilon"

    newSteward, newStewardWallet = newAdHocSteward

    # case from the ticket
    def _renameNodePortField(op):
        op[DATA].update({NODE_PORT + ' ': op[DATA][NODE_PORT]})
        del op[DATA][NODE_PORT]

    sendAddNewNode(newNodeName, newSteward, newStewardWallet,
                   transformOpFunc=_renameNodePortField)
    waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                  "unknown field")

    for fn in (NODE_IP, CLIENT_IP, NODE_PORT, CLIENT_PORT):
        def _tnf(op): del op[DATA][fn]
        sendAddNewNode(newNodeName, newSteward, newStewardWallet,
                       transformOpFunc=_tnf)
        # wait NAcks with exact message. it does not works for just 'is missed'
        # because the 'is missed' will check only first few cases
        waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, newSteward,
                                      "unknown field")


def testStewardCannotAddMoreThanOneNode(looper, txnPoolNodeSet, steward1,
                                        stewardWallet, tdirWithPoolTxns, tconf,
                                        allPluginsPath):
    newNodeName = "Epsilon"
    sendAddNewNode(newNodeName, steward1, stewardWallet)

    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1,
                             'already has a node',
                             node.clientstack.name)


def testNonStewardCannotAddNode(looper, txnPoolNodeSet, client1,
                                wallet1, client1Connected, tdirWithPoolTxns,
                                tconf, allPluginsPath):
    newNodeName = "Epsilon"
    sendAddNewNode(newNodeName, client1, wallet1)
    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, client1, 'is not a steward so cannot add a '
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

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chkNodeRegRecvd, retryWait=1, timeout=timeout))
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
                                                                     tdirWithPoolTxns,
                                                                     tconf,
                                                                     allPluginsPath)
        txnPoolNodeSet.append(newNode)
        looper.run(checkNodesConnected(txnPoolNodeSet))
        logger.debug("{} connected to the pool".format(newNode))
        waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1])

    f = getMaxFailures(len(txnPoolNodeSet))

    def checkFValue():
        for node in txnPoolNodeSet:
            assert node.f == f
            assert len(node.replicas) == (f + 1)

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(checkFValue, retryWait=1, timeout=timeout))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)


def testNodePortCannotBeChangedByAnotherSteward(looper, txnPoolNodeSet,
                                                tdirWithPoolTxns, tconf,
                                                steward1, stewardWallet,
                                                nodeThetaAdded):
    _, _, newNode = nodeThetaAdded
    nodeNewHa = genHa(1)
    new_port = nodeNewHa.port
    node_ha = txnPoolNodeSet[0].nodeReg[newNode.name]
    cli_ha = txnPoolNodeSet[0].cliNodeReg[newNode.name + CLIENT_STACK_SUFFIX]
    node_data = {
        ALIAS: newNode.name,
        NODE_PORT: new_port,
        NODE_IP: node_ha.host,
        CLIENT_PORT: cli_ha.port,
        CLIENT_IP: cli_ha.host,
    }

    logger.debug('{} changing port to {} {}'.format(newNode, new_port,
                                                    newNode.nodestack.ha.port))
    sendUpdateNode(steward1, stewardWallet, newNode,
                   node_data)

    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1, 'is not a steward of node',
                             node.clientstack.name)


def test_node_alias_cannot_be_changed(looper, txnPoolNodeSet,
                                      tdirWithPoolTxns,
                                      tconf, nodeThetaAdded):
    """
    The node alias cannot be changed.
    """
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    node_data = {ALIAS: 'foo'}
    sendUpdateNode(newSteward, newStewardWallet, newNode,
                   node_data)
    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, newSteward,
                             'data has conflicts with request data',
                             node.clientstack.name)


def testNodePortChanged(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        tconf, steward1, stewardWallet, nodeThetaAdded):
    """
    An running node's port is changed
    """
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    nodeNewHa = genHa(1)
    new_port = nodeNewHa.port

    node_ha = txnPoolNodeSet[0].nodeReg[newNode.name]
    cli_ha = txnPoolNodeSet[0].cliNodeReg[newNode.name + CLIENT_STACK_SUFFIX]
    node_data = {
        ALIAS: newNode.name,
        NODE_PORT: new_port,
        NODE_IP: node_ha.host,
        CLIENT_PORT: cli_ha.port,
        CLIENT_IP: cli_ha.host,
    }

    node = updateNodeDataAndReconnect(looper, newSteward,
                                    newStewardWallet, newNode,
                                    node_data,
                                    tdirWithPoolTxns, tconf,
                                    txnPoolNodeSet)

    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1])

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
    looper.removeProdable(name=newNode.name)
    nodeHa, nodeCHa = HA(*newNode.nodestack.ha), HA(*newNode.clientstack.ha)
    sigseed = randomString(32).encode()
    verkey = SimpleSigner(seed=sigseed).naclSigner.verhex.decode()
    changeNodeKeys(looper, newSteward, newStewardWallet, newNode, verkey)
    initNodeKeysForBothStacks(newNode.name, tdirWithPoolTxns, sigseed,
                              override=True)

    logger.debug("{} starting with HAs {} {}".format(newNode, nodeHa, nodeCHa))
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(node)
    # The last element of `txnPoolNodeSet` is the node Theta that was just
    # stopped
    txnPoolNodeSet[-1] = node
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1])
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)


