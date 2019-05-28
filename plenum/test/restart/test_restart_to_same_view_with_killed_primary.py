import pytest

from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange, view_change_timeout
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.restart.helper import restart_nodes
from plenum.test.test_node import ensureElectionsDone, ensure_node_disconnected

nodeCount = 7

VIEW_CHANGE_TIMEOUT = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, VIEW_CHANGE_TIMEOUT):
        old_network_3pc_watcher_state = tconf.ENABLE_INCONSISTENCY_WATCHER_NETWORK
        tconf.ENABLE_INCONSISTENCY_WATCHER_NETWORK = True
        yield tconf
        tconf.ENABLE_INCONSISTENCY_WATCHER_NETWORK = old_network_3pc_watcher_state


def test_restart_to_same_view_with_killed_primary(looper, txnPoolNodeSet, tconf, tdir, allPluginsPath,
                                                  sdk_pool_handle, sdk_wallet_client):
    restart_timeout = tconf.ToleratePrimaryDisconnection + \
                      waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))

    primary = txnPoolNodeSet[0]
    alive_nodes = txnPoolNodeSet[1:]
    minority = alive_nodes[-1:]
    majority = alive_nodes[:-1]

    # Move to higher view by killing primary
    primary.cleanupOnStopping = True
    primary.stop()
    looper.removeProdable(primary)
    ensure_node_disconnected(looper, primary, txnPoolNodeSet)
    waitForViewChange(looper, alive_nodes, 1, customTimeout=VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper, alive_nodes, instances_list=range(3))

    # Add transaction to ledger
    sdk_send_random_and_check(looper, alive_nodes, sdk_pool_handle, sdk_wallet_client, 1)

    # Restart majority group
    majority_before_restart = majority.copy()
    restart_nodes(looper, alive_nodes, majority, tconf, tdir, allPluginsPath,
                  after_restart_timeout=restart_timeout, start_one_by_one=False, wait_for_elections=False)
    waitForViewChange(looper, majority, 1, customTimeout=2.1 * VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper, majority, instances_list=range(3))

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
    restart_nodes(looper, alive_nodes, minority, tconf, tdir, allPluginsPath,
                  after_restart_timeout=restart_timeout, start_one_by_one=False, wait_for_elections=False)
    ensureElectionsDone(looper, alive_nodes, instances_list=range(3))

    # Check that all nodes are still functional
    sdk_ensure_pool_functional(looper, alive_nodes, sdk_wallet_client, sdk_pool_handle)
