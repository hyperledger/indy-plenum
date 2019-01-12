import pytest

from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.replica_removing.helper import check_replica_removed
from stp_core.loop.eventually import eventually
from plenum.test.helper import waitForViewChange
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected

nodeCount = 7


@pytest.fixture(scope="module")
def tconf(tconf):
    old_time = tconf.TolerateBackupPrimaryDisconnection
    tconf.TolerateBackupPrimaryDisconnection = 5
    yield tconf
    tconf.TolerateBackupPrimaryDisconnection = old_time


def test_replica_removing_after_view_change(looper,
                                            txnPoolNodeSet,
                                            sdk_pool_handle,
                                            sdk_wallet_client,
                                            tconf,
                                            tdir,
                                            allPluginsPath):
    """
    1. Remove backup primary node.
    2. Check that replicas with the disconnected primary were removed.
    3. Start View Change.
    4. Check that the new replica with disconnected primary is removed and
    other replicas are working correctly.
    5. Recover the removed node.
    6. Start View Change.
    7. Check that all replicas were restored.
    """
    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    instance_to_remove = txnPoolNodeSet[0].requiredNumberOfInstances - 1
    removed_node = txnPoolNodeSet[instance_to_remove]
    # remove backup primary node.
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, removed_node)
    txnPoolNodeSet.remove(removed_node)
    looper.removeProdable(removed_node)

    # check that replicas were removed
    def check_replica_removed_on_all_nodes(inst_id=instance_to_remove):
        for n in txnPoolNodeSet:
            check_replica_removed(n,
                                  start_replicas_count,
                                  inst_id)
            assert not n.monitor.isMasterDegraded()
            assert len(n.requests) == 0

    looper.run(eventually(check_replica_removed_on_all_nodes,
                          timeout=tconf.TolerateBackupPrimaryDisconnection * 2))

    # start View Change
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1,
                      customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
    instance_to_remove -= 1
    instances = list(range(txnPoolNodeSet[0].requiredNumberOfInstances))
    instances.remove(instance_to_remove)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
                        instances_list=instances,
                        customTimeout=tconf.TolerateBackupPrimaryDisconnection * 4)
    # check that all replicas were restored
    looper.run(eventually(check_replica_removed_on_all_nodes,
                          instance_to_remove,
                          timeout=tconf.TolerateBackupPrimaryDisconnection * 2))

    # recover the removed node
    removed_node = start_stopped_node(removed_node, looper, tconf,
                                      tdir, allPluginsPath)
    txnPoolNodeSet.append(removed_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    # start View Change
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
                        instances_list=range(txnPoolNodeSet[0].requiredNumberOfInstances),
                        customTimeout=tconf.TolerateBackupPrimaryDisconnection * 2)
    assert start_replicas_count == removed_node.replicas.num_replicas
