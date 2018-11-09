from stp_core.loop.eventually import eventually

from plenum.common.config_helper import PNodeConfigHelper

from plenum.test.test_node import ensure_node_disconnected, TestNode, ensureElectionsDone, checkNodesConnected

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check, \
    sdk_send_batches_of_random_and_check, checkViewNoForNodes

from plenum.test.primary_selection.helper import getPrimaryNodesIdxs

from plenum.test import waits
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.restart.helper import get_group, restart_nodes

nodeCount = 7
batches_count = 3


def get_primary_replicas(nodes):
    primary_replicas = []
    for instId in nodes[0].replicas.keys():
        for idx, node in enumerate(nodes):
            if node.replicas[instId].isPrimary:
                assert instId == len(primary_replicas)
                if node.replicas[instId].isMaster:
                    primary_replicas.insert(0, node.replicas[instId])
                else:
                    primary_replicas.append(node.replicas[instId])
    return primary_replicas


def test_persist_last_pp(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    primary_replicas = get_primary_replicas(txnPoolNodeSet)

    assert len(primary_replicas) == 3
    assert primary_replicas[0].isPrimary and primary_replicas[0].isMaster
    assert primary_replicas[1].isPrimary
    assert primary_replicas[2].isPrimary

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)

    # Check that we've persisted last send pre-prepare on every primary replica
    assert primary_replicas[0].last_ordered_3pc[1] == \
           primary_replicas[0].lastPrePrepareSeqNo
    assert primary_replicas[1].last_ordered_3pc == \
           (primary_replicas[1].lastPrePrepare.viewNo, primary_replicas[1].lastPrePrepare.ppSeqNo)
    assert primary_replicas[2].last_ordered_3pc == \
           (primary_replicas[2].lastPrePrepare.viewNo, primary_replicas[2].lastPrePrepare.ppSeqNo)


def test_restore_persisted_last_pp_after_restart(looper, txnPoolNodeSet, tconf, tdir,
                                                 sdk_pool_handle, sdk_wallet_client, allPluginsPath):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)
    primary_replicas = get_primary_replicas(txnPoolNodeSet)
    primary_replicas = primary_replicas[1].last_ordered_3pc
    primary_replicas = primary_replicas[2].last_ordered_3pc

    # Restart backup primaries
    restart_nodes(looper, txnPoolNodeSet, [primary_replicas[1].node, primary_replicas[2].node],
                  tconf, tdir, allPluginsPath)

    # Check that we've persisted last send pre-prepare on backup primary replicas
    assert primary_replicas[1].last_ordered_3pc == \
           (primary_replicas[1].lastPrePrepare.viewNo, primary_replicas[1].lastPrePrepare.ppSeqNo)
    assert primary_replicas[2].last_ordered_3pc == \
           (primary_replicas[2].lastPrePrepare.viewNo, primary_replicas[2].lastPrePrepare.ppSeqNo)


def test_clear_persisted_last_pp_after_view_change(looper, txnPoolNodeSet, tconf, tdir,
                                                   sdk_pool_handle, sdk_wallet_client, allPluginsPath):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)

    primary_replicas = get_primary_replicas(txnPoolNodeSet)
    # Restart master primary to make a view_change
    restart_nodes(looper, txnPoolNodeSet, [primary_replicas[0].node], tconf, tdir, allPluginsPath,
                  after_restart_timeout=tconf.ToleratePrimaryDisconnection + 1)
    assert primary_replicas[0] is not get_primary_replicas(txnPoolNodeSet)[0]

    # Check that we've cleared last send pre-prepare on every primary replica


def test_clear_persisted_last_pp_after_pool_restart(looper, txnPoolNodeSet, tconf, tdir,
                                                    sdk_pool_handle, sdk_wallet_client, allPluginsPath):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)

    primary_replicas = get_primary_replicas(txnPoolNodeSet)

    # Restart the pool
    for node in txnPoolNodeSet:
        node.stop()
        looper.removeProdable(node)

    # Starting nodes again by creating `Node` objects since that simulates
    # what happens when starting the node with script
    restartedNodes = []
    for node in txnPoolNodeSet:
        config_helper = PNodeConfigHelper(node.name, tconf, chroot=tdir)
        restartedNode = TestNode(node.name,
                                 config_helper=config_helper,
                                 config=tconf, ha=node.nodestack.ha,
                                 cliha=node.clientstack.ha)
        looper.add(restartedNode)
        restartedNodes.append(restartedNode)

    restNodes = [node for node in restartedNodes if node.name != primary_replicas[0].node.name]

    looper.run(checkNodesConnected(restNodes))
    ensureElectionsDone(looper, restNodes)
    checkViewNoForNodes(restNodes, 0)

    assert primary_replicas[0].last_ordered_3pc == (0, 0) and \
           primary_replicas[0].lastPrePrepare is None
    assert primary_replicas[1].last_ordered_3pc == (0, 0) and \
           primary_replicas[1].lastPrePrepare is None
    assert primary_replicas[2].last_ordered_3pc == (0, 0) and \
           primary_replicas[2].lastPrePrepare is None

    # Check that we've cleared last send pre-prepare on every primary replica
