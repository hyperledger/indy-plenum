import pytest
from plenum.client.client import Client
from stp_core.loop.eventually import eventually
from plenum.common.constants import CLIENT_STACK_SUFFIX
from plenum.common.util import hexToFriendly
from plenum.server.node import Node
from plenum.test.helper import waitForSufficientRepliesForRequests
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import demote_node, \
    buildPoolClientAndWallet, promote_node
from plenum.test.test_node import TestNode, checkNodesConnected


def checkNodeNotInNodeReg(node, nodeName):
    if isinstance(node, Node):
        assert nodeName not in node.nodeReg
        assert nodeName not in node.nodestack.connecteds
    else:
        raise ValueError("pass a node or client object as first argument")


@pytest.mark.skip(reason="SOV-383")
def testStewardSuspendsNode(looper, txnPoolNodeSet,
                            tdirWithPoolTxns, tconf,
                            sdk_pool_handle,
                            sdk_wallet_steward,
                            sdk_node_theta_added,
                            poolTxnStewardData,
                            allPluginsPath):
    new_steward_wallet, new_node = sdk_node_theta_added
    newNodeNym = hexToFriendly(new_node.nodestack.verhex)
    demote_node(looper, newSteward, newStewardWallet, newNodeNym, new_node.name)
    # Check suspended node does not exist in any nodeReg or remotes of
    # nodes or clients

    txnPoolNodeSet = txnPoolNodeSet[:-1]
    for node in txnPoolNodeSet:
        looper.run(eventually(checkNodeNotInNodeReg, node, new_node.name))

    # Check a client can send request and receive replies
    # req = sendRandomRequest(newStewardWallet, newSteward)
    # waitForSufficientRepliesForRequests(looper, newSteward,
    #                                     requests=[req])

    # Check that a restarted client or node does not connect to the suspended
    # node
    steward1.stop()
    looper.removeProdable(steward1)
    steward1, stewardWallet = buildPoolClientAndWallet(poolTxnStewardData,
                                                       tdirWithPoolTxns)
    looper.add(steward1)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    looper.run(eventually(checkNodeNotInNodeReg, steward1, new_node.name))

    new_node.stop()
    looper.removeProdable(new_node)

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
    looper.run(eventually(checkNodeNotInNodeReg, oldNode, new_node.name))

    # Check that a node whose suspension is revoked can reconnect to other
    # nodes and clients can also connect to that node
    promote_node(looper, newSteward, newStewardWallet, newNodeNym,
                 new_node.name)
    nodeTheta = TestNode(new_node.name, basedirpath=tdirWithPoolTxns, base_data_dir=tdirWithPoolTxns,
                         config=tconf, pluginPaths=allPluginsPath,
                         ha=new_node.nodestack.ha, cliha=new_node.clientstack.ha)
    looper.add(nodeTheta)
    txnPoolNodeSet.append(nodeTheta)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)
