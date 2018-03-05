import pytest
from plenum.client.client import Client
from stp_core.loop.eventually import eventually
from plenum.common.constants import CLIENT_STACK_SUFFIX
from plenum.common.util import hexToFriendly
from plenum.server.node import Node
from plenum.test.helper import sendRandomRequest, \
    waitForSufficientRepliesForRequests
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import suspendNode, \
    buildPoolClientAndWallet, cancelNodeSuspension
from plenum.test.test_node import TestNode, checkNodesConnected


def checkNodeNotInNodeReg(nodeOrClient, nodeName):
    if isinstance(nodeOrClient, Node):
        assert nodeName not in nodeOrClient.nodeReg
        assert nodeName not in nodeOrClient.nodestack.connecteds
    elif isinstance(nodeOrClient, Client):
        clientStackName = nodeName + CLIENT_STACK_SUFFIX
        assert clientStackName not in nodeOrClient.nodeReg
        assert clientStackName not in nodeOrClient.nodestack.connecteds
    else:
        raise ValueError("pass a node or client object as first argument")


@pytest.mark.skip(reason="SOV-383")
def testStewardSuspendsNode(looper, txnPoolNodeSet,
                            tdirWithPoolTxns, tconf,
                            steward1, stewardWallet,
                            nodeThetaAdded,
                            poolTxnStewardData,
                            allPluginsPath):
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    newNodeNym = hexToFriendly(newNode.nodestack.verhex)
    suspendNode(looper, newSteward, newStewardWallet, newNodeNym, newNode.name)
    # Check suspended node does not exist in any nodeReg or remotes of
    # nodes or clients

    txnPoolNodeSet = txnPoolNodeSet[:-1]
    for node in txnPoolNodeSet:
        looper.run(eventually(checkNodeNotInNodeReg, node, newNode.name))
    for client in (steward1, newSteward):
        looper.run(eventually(checkNodeNotInNodeReg, client, newNode.name))

    # Check a client can send request and receive replies
    req = sendRandomRequest(newStewardWallet, newSteward)
    waitForSufficientRepliesForRequests(looper, newSteward,
                                        requests=[req])

    # Check that a restarted client or node does not connect to the suspended
    # node
    steward1.stop()
    looper.removeProdable(steward1)
    steward1, stewardWallet = buildPoolClientAndWallet(poolTxnStewardData,
                                                       tdirWithPoolTxns)
    looper.add(steward1)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    looper.run(eventually(checkNodeNotInNodeReg, steward1, newNode.name))

    newNode.stop()
    looper.removeProdable(newNode)

    # TODO: There is a bug that if a primary node is turned off, it sends
    # duplicate Pre-Prepare and gets blacklisted. Here is the gist
    # https://gist.github.com/lovesh/c16989616ebb6856f9fa2905c14dc4b7
    oldNodeIdx, oldNode = [(i, n) for i, n in enumerate(txnPoolNodeSet)
                           if not n.hasPrimary][0]
    oldNode.stop()
    looper.removeProdable(oldNode)
    oldNode = TestNode(oldNode.name, basedirpath=tdirWithPoolTxns, base_data_dir=tdirWithPoolTxns,
                       config=tconf, pluginPaths=allPluginsPath)
    looper.add(oldNode)
    txnPoolNodeSet[oldNodeIdx] = oldNode
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.run(eventually(checkNodeNotInNodeReg, oldNode, newNode.name))

    # Check that a node whose suspension is revoked can reconnect to other
    # nodes and clients can also connect to that node
    cancelNodeSuspension(looper, newSteward, newStewardWallet, newNodeNym,
                         newNode.name)
    nodeTheta = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, base_data_dir=tdirWithPoolTxns,
                         config=tconf, pluginPaths=allPluginsPath,
                         ha=newNode.nodestack.ha, cliha=newNode.clientstack.ha)
    looper.add(nodeTheta)
    txnPoolNodeSet.append(nodeTheta)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)
