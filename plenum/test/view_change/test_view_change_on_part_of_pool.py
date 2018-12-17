import pytest as pytest

from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.restart.helper import restart_nodes
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected
from plenum.test.view_change.helper import ensure_view_change, start_stopped_node
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_interval = tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL

    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = 15
    yield tconf

    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = old_interval


def restart_node(looper, txnPoolNodeSet, node_to_disconnect, tconf, tdir,
                 allPluginsPath):
    idx = txnPoolNodeSet.index(node_to_disconnect)
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_disconnect)
    looper.removeProdable(name=node_to_disconnect.name)

    # add node_to_disconnect to pool
    node_to_disconnect = start_stopped_node(node_to_disconnect, looper, tconf,
                                            tdir, allPluginsPath)
    txnPoolNodeSet[idx] = node_to_disconnect
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet)


def test_view_change_not_happen_with_different_ic(looper, txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client,
                                     tconf, tdir, allPluginsPath):
    """
    Check that view change is done after processing some of txns
    """
    nodes_to_restart = txnPoolNodeSet[1:3]
    panic_node = txnPoolNodeSet[-1]
    view_no = txnPoolNodeSet[0].viewNo

    panic_node.view_changer.on_master_degradation()
    for n in nodes_to_restart:
        restart_node(looper, txnPoolNodeSet, n, tconf, tdir, allPluginsPath)
    nodes_to_restart = txnPoolNodeSet[1:3]

    for n in nodes_to_restart:
        n.view_changer.on_master_degradation()

    def check():
        params = panic_node.view_changer.spylog.getLastParams(
            panic_node.view_changer._canViewChange)
        assert params and params["proposedViewNo"] == 1
    looper.run(eventually(check))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert node.viewNo == view_no


def test_view_change_not_happen_if_ic_is_discarded(looper, txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client,
                                     tconf, tdir, allPluginsPath):
    """
    Check that view change is done after processing some of txns
    """
    nodes_to_restart = txnPoolNodeSet[1:3]
    panic_node = txnPoolNodeSet[-1]
    view_no = txnPoolNodeSet[0].viewNo

    panic_node.view_changer.on_master_degradation()
    for n in nodes_to_restart:
        restart_node(looper, txnPoolNodeSet, n, tconf, tdir, allPluginsPath)
    nodes_to_restart = txnPoolNodeSet[1:3]

    # waiting to discard InstanceChange
    looper.runFor(tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL)

    for n in nodes_to_restart:
        n.view_changer.on_master_degradation()

    def check():
        params = panic_node.view_changer.spylog.getLastParams(
            panic_node.view_changer._canViewChange)
        assert params and params["proposedViewNo"] == 1

    looper.run(eventually(check))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert node.viewNo == view_no
