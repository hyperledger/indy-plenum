import copy
from contextlib import contextmanager

import pytest
from plenum.common.constants import STEWARD_STRING, VALIDATOR

from plenum.test import waits
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected
from plenum.test.view_change.test_view_change_timeout import start_view_change

from plenum.common.util import randomString
from plenum.test.view_change.helper import add_new_node

from plenum.test.helper import sdk_send_random_and_check, checkViewNoForNodes
from plenum.test.pool_transactions.helper import demote_node, sdk_add_new_nym, sdk_pool_refresh, sdk_add_new_node

nodeCount = 6

old_commit = None


def test_future_primaries_replicas_increase(looper, txnPoolNodeSet, sdk_pool_handle,
                                            sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    # Don't delete NodeStates, so we could check them.
    global old_commit
    old_commit = txnPoolNodeSet[0].future_primaries_handler.commit_batch
    for node in txnPoolNodeSet:
        node.future_primaries_handler.commit_batch = lambda three_pc_batch, prev_handler_result=None: 0

    initial_primaries = copy.copy(txnPoolNodeSet[0].primaries)
    last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    starting_view_number = checkViewNoForNodes(txnPoolNodeSet)

    # Increase replicas count
    add_new_node(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_stewards[0], tdir, tconf, allPluginsPath)

    new_view_no = checkViewNoForNodes(txnPoolNodeSet)
    assert new_view_no == starting_view_number + 1
    # "seq_no + 2" because 1 domain and 1 pool txn.
    state = txnPoolNodeSet[0].future_primaries_handler.node_states[(last_ordered[0], last_ordered[1] + 2)]
    assert len(state.primaries) == len(initial_primaries) + 1
    assert len(state.primaries) == len(txnPoolNodeSet[0].primaries)
    # Because view_change happened
    assert state.primaries != txnPoolNodeSet[0].primaries


def test_future_primaries_replicas_decrease(looper, txnPoolNodeSet, sdk_pool_handle,
                                            sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    assert len(txnPoolNodeSet) == 7

    initial_primaries = copy.copy(txnPoolNodeSet[0].primaries)
    last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    starting_view_number = checkViewNoForNodes(txnPoolNodeSet)

    # Decrease replicas count
    demote_node(looper, sdk_wallet_stewards[-1], sdk_pool_handle, txnPoolNodeSet[-2])
    txnPoolNodeSet.remove(txnPoolNodeSet[-2])
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    new_view_no = checkViewNoForNodes(txnPoolNodeSet)
    assert new_view_no == starting_view_number + 1
    state = txnPoolNodeSet[0].future_primaries_handler.node_states[(last_ordered[0], last_ordered[1] + 1)]
    assert len(state.primaries) + 1 == len(initial_primaries)
    assert len(state.primaries) == len(txnPoolNodeSet[0].primaries)
    # Because view_change happened
    assert state.primaries != txnPoolNodeSet[0].primaries
