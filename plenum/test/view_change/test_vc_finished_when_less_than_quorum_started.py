import time

import pytest

from plenum.test import waits
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances, checkNodesConnected, TestViewChanger
from plenum.test.view_change.helper import ensure_view_change, start_stopped_node


def test_vc_finished_when_less_than_quorum_started(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle, tconf, tdir, allPluginsPath):

    checkViewNoForNodes(txnPoolNodeSet, 0)

    alpha, beta, gamma, delta = txnPoolNodeSet

    # Delta and Gamma send InstanceChange for all nodes.
    for node in [gamma, delta]:
        node.view_changer.on_master_degradation()

    # Restart Alpha, Beta, Gamma
    for i, node in enumerate([alpha, beta, gamma]):
        node = _restart_node(node, looper, txnPoolNodeSet,
                                 tconf, tdir, allPluginsPath)
        txnPoolNodeSet[i] = node
    looper.run(checkNodesConnected(txnPoolNodeSet))
    alpha, beta, gamma, delta = txnPoolNodeSet

    # Send InstanceChange from Beta for all nodes
    beta.view_changer.on_master_degradation()

    # Ensure that pool is still functional
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # Delta and Gamma send InstanceChange for all nodes.
    for node in [gamma, delta]:
        node.view_changer.on_master_degradation()

    ensureElectionsDone(looper, txnPoolNodeSet)

    # Ensure that pool is still functional
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


def _restart_node(node, looper, txnPoolNodeSet,
                  tconf, tdir, allPluginsPath):
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node.name,
                                            stopNode=True)
    looper.removeProdable(node)

    new_node = start_stopped_node(node, looper,
                              tconf, tdir, allPluginsPath)
    new_node.view_changer = TestViewChanger(new_node)
    return new_node
