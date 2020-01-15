import pytest

from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.replica_removing.helper import check_replica_removed
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually
from plenum.test.helper import waitForViewChange
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected


@pytest.fixture(scope="module")
def tconf(tconf):
    old_time = tconf.TolerateBackupPrimaryDisconnection
    tconf.TolerateBackupPrimaryDisconnection = 5
    yield tconf
    tconf.TolerateBackupPrimaryDisconnection = old_time


def test_replica_removing_with_primary_disconnected(looper,
                                                    txnPoolNodeSet,
                                                    sdk_pool_handle,
                                                    sdk_wallet_client,
                                                    tconf,
                                                    tdir,
                                                    allPluginsPath):
    """
    1. Remove backup primary node.
    2. Check that replicas with the disconnected primary were removed.
    3. Recover the removed node.
    4. Start View Change.
    5. Check that all replicas were restored.
    """
    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    instance_to_remove = 1
    node = txnPoolNodeSet[instance_to_remove]
    # remove backup primary node.
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node)
    txnPoolNodeSet.remove(node)
    looper.removeProdable(node)

    # check that replicas were removed
    def check_replica_removed_on_all_nodes():
        for node in txnPoolNodeSet:
            check_replica_removed(node,
                                  start_replicas_count,
                                  instance_to_remove)
    looper.run(eventually(check_replica_removed_on_all_nodes,
                          timeout=tconf.TolerateBackupPrimaryDisconnection * 4))
    assert not node.monitor.isMasterDegraded()
    assert len(node.requests) == 0

    # recover the removed node
    node = start_stopped_node(node, looper, tconf,
                              tdir, allPluginsPath)
    txnPoolNodeSet.append(node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    # start View Change
    trigger_view_change(txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1,
                      customTimeout=2 * tconf.NEW_VIEW_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # check that all replicas were restored
    assert start_replicas_count == node.replicas.num_replicas
