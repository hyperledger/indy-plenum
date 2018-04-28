import pytest

from stp_core.network.exceptions import RemoteNotFound

from plenum.test.helper import sendMessageAndCheckDelivery

from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.view_change.helper import start_stopped_node

from stp_core.loop.eventually import eventually
from plenum.server.node import Node
from plenum.test.pool_transactions.helper import demote_node, \
    promote_node, sdk_pool_refresh
from plenum.test.test_node import checkNodesConnected


def checkNodeNotInNodeReg(node, nodeName):
    if isinstance(node, Node):
        assert nodeName not in node.nodeReg
        assert nodeName not in node.nodestack.connecteds
    else:
        raise ValueError("pass a node or client object as first argument")


def testStewardSuspendsNode(looper, txnPoolNodeSet,
                            tdir, tconf,
                            sdk_pool_handle,
                            sdk_wallet_steward,
                            sdk_node_theta_added,
                            poolTxnStewardData,
                            allPluginsPath):
    new_steward_wallet, new_node = sdk_node_theta_added
    demote_node(looper, new_steward_wallet, sdk_pool_handle, new_node)
    # Check suspended node does not exist in any nodeReg or remotes of
    # nodes or clients

    txnPoolNodeSet = txnPoolNodeSet[:-1]
    for node in txnPoolNodeSet:
        looper.run(eventually(checkNodeNotInNodeReg, node, new_node.name))
    # Check that a node does not connect to the suspended
    # node
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, new_steward_wallet, sdk_pool_handle)
    with pytest.raises(RemoteNotFound):
        looper.loop.run_until_complete(sendMessageAndCheckDelivery(txnPoolNodeSet[0], new_node))

    new_node.stop()
    looper.removeProdable(new_node)

    # Check that a node whose suspension is revoked can reconnect to other
    # nodes and clients can also connect to that node

    promote_node(looper, new_steward_wallet, sdk_pool_handle, new_node)
    nodeTheta = start_stopped_node(new_node, looper, tconf,
                                   tdir, allPluginsPath,
                                   delay_instance_change_msgs=False)
    txnPoolNodeSet.append(nodeTheta)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
