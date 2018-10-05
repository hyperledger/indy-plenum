import pytest

from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.restart.helper import restart_nodes
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change_complete

nodeCount = 4
TestRunningTimeLimitSec = 150


@pytest.fixture(scope="module")
def tconf(tconf):
    old_network_3pc_watcher_state = tconf.ENABLE_INCONSISTENCY_WATCHER_NETWORK
    tconf.ENABLE_INCONSISTENCY_WATCHER_NETWORK = True
    yield tconf
    tconf.ENABLE_INCONSISTENCY_WATCHER_NETWORK = old_network_3pc_watcher_state


def test_restart_majority_to_same_view(looper, txnPoolNodeSet, tconf, tdir, allPluginsPath,
                                        sdk_pool_handle, sdk_wallet_client):
    # Add transaction to ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    majority = txnPoolNodeSet[:3]
    minority = txnPoolNodeSet[3:]

    # Restart majority group
    tm = tconf.ToleratePrimaryDisconnection + waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))
    majority_before_restart = majority.copy()
    restart_nodes(looper, txnPoolNodeSet, majority, tconf, tdir, allPluginsPath,
                  after_restart_timeout=tm, start_one_by_one=False, wait_for_elections=False)
    ensureElectionsDone(looper, majority, instances_list=range(2))

    # Check that nodes in minority group are aware that they might have inconsistent 3PC state
    for node in minority:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 1

    # Check that nodes in majority group didn't think they might have inconsistent 3PC state
    for node in majority_before_restart:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 0

    # Check that nodes in majority group don't think they might have inconsistent 3PC state
    for node in majority:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 0

    # Restart minority group
    restart_nodes(looper, txnPoolNodeSet, minority, tconf, tdir, allPluginsPath,
                  after_restart_timeout=tm, start_one_by_one=False)

    # Check that all nodes are still functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_restart_majority_to_lower_view(looper, txnPoolNodeSet, tconf, tdir, allPluginsPath,
                                        sdk_pool_handle, sdk_wallet_client):
    # Add transaction to ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    # Move to higher view
    ensure_view_change_complete(looper, txnPoolNodeSet)

    majority = txnPoolNodeSet[:3]
    minority = txnPoolNodeSet[3:]

    # Restart majority group
    tm = tconf.ToleratePrimaryDisconnection + waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))
    majority_before_restart = majority.copy()
    restart_nodes(looper, txnPoolNodeSet, majority, tconf, tdir, allPluginsPath,
                  after_restart_timeout=tm, start_one_by_one=False, wait_for_elections=False)
    ensureElectionsDone(looper, majority, instances_list=range(2))

    # Check that nodes in minority group are aware that they might have inconsistent 3PC state
    for node in minority:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 1

    # Check that nodes in majority group didn't think they might have inconsistent 3PC state
    for node in majority_before_restart:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 0

    # Check that nodes in majority group don't think they might have inconsistent 3PC state
    for node in majority:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 0

    # Restart minority group
    restart_nodes(looper, txnPoolNodeSet, minority, tconf, tdir, allPluginsPath,
                  after_restart_timeout=tm, start_one_by_one=False)

    # Check that all nodes are still functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_restart_half_to_lower_view(looper, txnPoolNodeSet, tconf, tdir, allPluginsPath,
                                    sdk_pool_handle, sdk_wallet_client):
    # Add transaction to ledger
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    # Move to higher view
    ensure_view_change_complete(looper, txnPoolNodeSet)

    # Restart half of nodes
    tm = tconf.ToleratePrimaryDisconnection + waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))
    nodes_before_restart = txnPoolNodeSet.copy()
    restart_nodes(looper, txnPoolNodeSet, txnPoolNodeSet[2:], tconf, tdir, allPluginsPath,
                  after_restart_timeout=tm, start_one_by_one=False)

    # Check that nodes didn't think they may have inconsistent 3PC state
    for node in nodes_before_restart:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 0

    # Check that nodes don't think they may have inconsistent 3PC state
    for node in txnPoolNodeSet:
        assert node.spylog.count(node.on_inconsistent_3pc_state) == 0

    # Check that all nodes are still functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle, num_reqs=2, num_batches=1)
