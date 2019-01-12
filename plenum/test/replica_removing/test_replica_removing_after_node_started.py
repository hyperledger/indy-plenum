import pytest

from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected, sdk_add_new_steward_and_node
from plenum.test.replica_removing.helper import check_replica_removed
from stp_core.loop.eventually import eventually
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected

nodeCount = 7


@pytest.fixture(scope="module")
def tconf(tconf):
    old_time = tconf.TolerateBackupPrimaryDisconnection
    old_strategy = tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED

    tconf.TolerateBackupPrimaryDisconnection = 5
    # It should be local because in a quorum logic a new node
    # will not be able to have a quorum of messages after start.
    tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "local"
    yield tconf

    tconf.TolerateBackupPrimaryDisconnection = old_time
    tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = old_strategy


def test_replica_removing_after_node_started(looper,
                                             txnPoolNodeSet,
                                             sdk_pool_handle,
                                             sdk_wallet_client,
                                             tconf,
                                             tdir,
                                             allPluginsPath,
                                             sdk_wallet_steward):
    """
    1. Remove backup primary node.
    2. Check that replicas with the disconnected primary were removed.
    3. Add new node
    4. Check that in the new node the replica with the disconnected primary were removed.
    3. Recover the removed node.
    4. Start View Change.
    5. Check that all replicas were restored.
    """
    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    instance_to_remove = txnPoolNodeSet[0].requiredNumberOfInstances - 1
    removed_primary_node = txnPoolNodeSet[instance_to_remove]
    # remove backup primary node.
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, removed_primary_node)
    txnPoolNodeSet.remove(removed_primary_node)
    looper.removeProdable(removed_primary_node)

    # check that replicas were removed
    def check_replica_removed_on_all_nodes(inst_id=instance_to_remove):
        for node in txnPoolNodeSet:
            check_replica_removed(node,
                                  start_replicas_count,
                                  inst_id)
            assert not node.monitor.isMasterDegraded()
            assert len(node.requests) == 0

    looper.run(eventually(check_replica_removed_on_all_nodes,
                          timeout=tconf.TolerateBackupPrimaryDisconnection * 2))

    new_steward_wallet, new_node = sdk_add_new_steward_and_node(looper,
                                                                sdk_pool_handle,
                                                                sdk_wallet_steward,
                                                                "test_steward",
                                                                "test_node",
                                                                tdir,
                                                                tconf,
                                                                allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
    looper.run(eventually(check_replica_removed,
                          new_node,
                          start_replicas_count,
                          instance_to_remove,
                          timeout=tconf.TolerateBackupPrimaryDisconnection * 2))

    # recover the removed node
    removed_primary_node = start_stopped_node(removed_primary_node, looper, tconf,
                                              tdir, allPluginsPath)
    txnPoolNodeSet.append(removed_primary_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    # start View Change
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
                        instances_list=range(txnPoolNodeSet[0].requiredNumberOfInstances),
                        customTimeout=tconf.TolerateBackupPrimaryDisconnection * 2)
    assert start_replicas_count == removed_primary_node.replicas.num_replicas
