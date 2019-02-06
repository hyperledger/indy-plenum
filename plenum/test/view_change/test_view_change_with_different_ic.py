import pytest as pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_interval = tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL

    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = 5
    yield tconf

    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = old_interval


@pytest.mark.skip("INDY-1911")
def test_view_change_with_different_ic(looper, txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       tconf, tdir, allPluginsPath):
    """
    1. panic_node (Delta) send InstanceChange for all nodes.
    2. Restart nodes_to_restart (Beta, Gamma).
    3. nodes_to_restart send InstanceChanges for all nodes.
    4. Ensure elections done.
    """
    nodes_to_restart = txnPoolNodeSet[1:3]
    panic_node = txnPoolNodeSet[-1]
    view_no = txnPoolNodeSet[0].viewNo

    panic_node.view_changer.on_master_degradation()
    for n in nodes_to_restart:
        _restart_node(looper, txnPoolNodeSet, n, tconf, tdir, allPluginsPath)
    nodes_to_restart = txnPoolNodeSet[1:3]

    for n in nodes_to_restart:
        n.view_changer.on_master_degradation()

    def check():
        assert panic_node.view_change_in_progress

    looper.run(eventually(check))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert node.viewNo > view_no


def test_view_change_not_happen_if_ic_is_discarded(looper, txnPoolNodeSet,
                                                   sdk_pool_handle,
                                                   sdk_wallet_client,
                                                   tconf, tdir, allPluginsPath):
    """
    1. panic_node (Delta) send InstanceChange for all nodes.
    2. Restart nodes_to_restart (Beta, Gamma).
    3. Wait OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL sec.
    4. nodes_to_restart send InstanceChanges for all nodes.
    5. Ensure elections done.
    """
    nodes_to_restart = txnPoolNodeSet[1:3]
    panic_node = txnPoolNodeSet[-1]
    view_no = txnPoolNodeSet[0].viewNo

    panic_node.view_changer.on_master_degradation()
    for n in nodes_to_restart:
        _restart_node(looper, txnPoolNodeSet, n, tconf, tdir, allPluginsPath)
    nodes_to_restart = txnPoolNodeSet[1:3]

    # waiting to discard InstanceChange
    def check_old_ic_discarded():
        assert all(not n.view_changer.instanceChanges.has_inst_chng_from(view_no + 1, panic_node.name)
                   for n in txnPoolNodeSet)

    looper.run(eventually(check_old_ic_discarded, timeout=tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1))

    for n in nodes_to_restart:
        n.view_changer.on_master_degradation()

    def check_ic():
        for node in txnPoolNodeSet:
            assert all(node.view_changer.instanceChanges.has_inst_chng_from(view_no + 1, n.name)
                       for n in nodes_to_restart)

    looper.run(eventually(check_ic))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert node.viewNo == view_no


def _restart_node(looper, txnPoolNodeSet, node_to_disconnect, tconf, tdir,
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
