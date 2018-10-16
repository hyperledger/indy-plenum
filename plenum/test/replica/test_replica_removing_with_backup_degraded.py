import pytest

from plenum.test.replica.helper import check_replica_removed
from stp_core.loop.eventually import eventually
from plenum.test.helper import waitForViewChange
from plenum.test.test_node import ensureElectionsDone


def test_replica_removing_with_backup_degraded(looper,
                                               txnPoolNodeSet,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               tconf,
                                               tdir,
                                               allPluginsPath):
    """
    1. Start backup degraded.
    2. Check that degraded replicas
    3. Start View Change.
    4. Check that all replicas were restored.
    """
    instance_to_remove = 1
    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    for node in txnPoolNodeSet:
        node.view_changer.on_backup_degradation([instance_to_remove])

    # check that replicas were removed
    def check_replica_removed_on_all_nodes():
        for node in txnPoolNodeSet:
            check_replica_removed(node,
                                  start_replicas_count,
                                  instance_to_remove)

    looper.run(eventually(check_replica_removed_on_all_nodes,
                          timeout=tconf.TolerateBackupPrimaryDisconnection * 4))
    for node in txnPoolNodeSet:
        assert not node.monitor.isMasterDegraded()
        assert len(node.requests) == 0

    # start View Change
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1,
                      customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # check that all replicas were restored
    assert all(start_replicas_count == node.replicas.num_replicas
               for node in txnPoolNodeSet)
