import pytest

from plenum.test.helper import sdk_send_random_and_check, waitForViewChange, view_change_timeout
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import demote_node, disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node

nodeCount = 6


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, 5):
        yield tconf


def test_demote_backup_primary(looper, txnPoolNodeSet, sdk_pool_handle,
                               sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    assert len(txnPoolNodeSet) == 6
    view_no = txnPoolNodeSet[-1].viewNo

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_stewards[0], 1)

    node_to_restart = txnPoolNodeSet[-1]
    node_to_demote = steward_for_demote_node = demote_node_index = None
    steward_for_demote_node = None
    for i, n in enumerate(txnPoolNodeSet):
        if n.name == txnPoolNodeSet[0].primaries[1]:
            node_to_demote = n
            steward_for_demote_node = sdk_wallet_stewards[i]
            demote_node_index = i
            break

    assert node_to_demote

    demote_node(looper, steward_for_demote_node, sdk_pool_handle,
                node_to_demote)
    del txnPoolNodeSet[demote_node_index]

    # we are expecting 2 view changes here since Beta is selected as a master Primary on view=1
    # (since node reg at the beginning of view 0 is used to select it), but it's not available (demoted),
    # so we do view change to view=2 by timeout
    waitForViewChange(looper, txnPoolNodeSet, view_no + 2)
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node_to_restart)
    looper.removeProdable(name=node_to_restart.name)
    node_to_restart = start_stopped_node(node_to_restart, looper, tconf,
                                         tdir, allPluginsPath)
    txnPoolNodeSet[-1] = node_to_restart
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_stewards[0], 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=20)
