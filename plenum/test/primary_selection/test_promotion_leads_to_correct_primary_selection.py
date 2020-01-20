import json

import pytest
from indy.did import create_and_store_my_did
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.test.node_catchup.test_config_ledger import start_stopped_node

from plenum.test.helper import sdk_send_random_and_check, checkViewNoForNodes, waitForViewChange
from plenum.test.pool_transactions.helper import demote_node, disconnect_node_and_ensure_disconnected, promote_node
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected

nodeCount = 7


def test_promotion_leads_to_correct_primary_selection(looper,
                                                      txnPoolNodeSet,
                                                      tdir,
                                                      tconf,
                                                      allPluginsPath,
                                                      sdk_wallet_stewards,
                                                      sdk_pool_handle):
    # We are saving pool state at moment of last view_change to send it
    # to newly connected nodes so they could restore primaries basing on this node set.
    # When current primaries getting edited because of promotion/demotion we don't take this into account.
    # That lead us to primary inconsistency on different nodes

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 1)
    assert txnPoolNodeSet[0].master_replica.isPrimary
    assert txnPoolNodeSet[1].replicas[1].isPrimary
    assert txnPoolNodeSet[2].replicas[2].isPrimary
    starting_view_number = checkViewNoForNodes(txnPoolNodeSet)

    node_1 = txnPoolNodeSet[0]
    node_3 = txnPoolNodeSet[2]

    # Demote node 3
    steward_3 = sdk_wallet_stewards[2]
    demote_node(looper, steward_3, sdk_pool_handle, node_3)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node_3)
    looper.removeProdable(node_3)
    txnPoolNodeSet.remove(node_3)

    # Checking that view change happened
    waitForViewChange(looper, txnPoolNodeSet, starting_view_number + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    assert all(node.replicas.primary_name_by_inst_id ==
               node_1.replicas.primary_name_by_inst_id
               for node in txnPoolNodeSet)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 2)
    for node in txnPoolNodeSet:
        assert node.f == 1
        assert node.replicas.num_replicas == 2

    # restart Node1
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node_1)
    looper.removeProdable(node_1)
    txnPoolNodeSet.remove(node_1)

    node_1 = start_stopped_node(node_1, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(node_1)

    # Wait so node_1 could start and catch up
    waitForViewChange(looper, txnPoolNodeSet, starting_view_number + 1)
    assert all(node.replicas.primary_name_by_inst_id ==
               node_1.replicas.primary_name_by_inst_id
               for node in txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # Promoting node 3, increasing replica count
    node_3 = start_stopped_node(node_3, looper, tconf, tdir, allPluginsPath)
    promote_node(looper, steward_3, sdk_pool_handle, node_3)
    txnPoolNodeSet.append(node_3)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Wait for view change after promotion
    waitForViewChange(looper, txnPoolNodeSet, starting_view_number + 2)
    ensureElectionsDone(looper, txnPoolNodeSet, instances_list=[0, 1, 2])

    # Node 3 able to do ordering
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], 2)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
