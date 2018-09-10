import pytest

from plenum.test.delayers import cDelay
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import reconnect_node_and_ensure_connected, \
    disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_requests, sdk_get_replies, sdk_send_random_and_check, waitForViewChange
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected

from plenum.test.checkpoints.conftest import chkFreqPatched

logger = getlogger()
CHK_FREQ = 1


def test_replica_removing(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    1. Remove replica
    2. Ordering
    3. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    index = start_replicas_count - 1
    node.replicas.remove_replica(index)
    _check_replica_removed(node, start_replicas_count)
    # trigger view change on all nodes
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)


def test_replica_removing_before_vc_with_primary_disconnected(looper,
                                                              txnPoolNodeSet,
                                                              sdk_pool_handle,
                                                              sdk_wallet_client,
                                                              tconf,
                                                              tdir,
                                                              allPluginsPath):
    """
    1. Remove replica
    2. Reconnect master primary
    3. Check that nodes and replicas correctly added
    """
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    index = start_replicas_count - 1
    node.replicas.remove_replica(index)
    _check_replica_removed(node, start_replicas_count)
    # trigger view change on all nodes
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node)
    txnPoolNodeSet.remove(node)
    looper.removeProdable(node)
    node = start_stopped_node(node, looper, tconf,
                              tdir, allPluginsPath)
    txnPoolNodeSet.append(node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1,
                      customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)


def test_replica_removing_before_ordering(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, chkFreqPatched):
    """
    1. Remove replica
    2. Ordering
    3. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    index = start_replicas_count - 1
    node.replicas.remove_replica(index)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    looper.run(eventually(check_checkpoint_finalize, txnPoolNodeSet))
    _check_replica_removed(node, start_replicas_count)
    # trigger view change on all nodes for return backup replicas
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)


def test_replica_removing_in_ordering(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    1. Start ordering (send pre-prepares on backup)
    2. Remove replica
    3. Finish ordering
    4. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    index = start_replicas_count - 1

    slow_stasher = node.nodeIbStasher
    with delay_rules(slow_stasher, cDelay()):
        req = sdk_send_random_requests(looper,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       1)
        node.replicas.remove_replica(index)
    sdk_get_replies(looper, req)
    looper.run(eventually(check_checkpoint_finalize, txnPoolNodeSet))
    _check_replica_removed(node, start_replicas_count)
    # trigger view change on all nodes for return backup replicas
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)


def test_replica_removing_after_ordering(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    1. Ordering
    2. Remove replica
    3. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    looper.run(eventually(check_checkpoint_finalize, txnPoolNodeSet))
    index = start_replicas_count - 1
    node.replicas.remove_replica(index)
    _check_replica_removed(node, start_replicas_count)
    # trigger view change on all nodes for return backup replicas
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)


def _check_replica_removed(node, start_replicas_count):
    replicas_count = start_replicas_count - 1
    assert node.replicas.num_replicas == replicas_count
    if node.monitor.acc_monitor is not None:
        assert node.monitor.acc_monitor == replicas_count
    assert not node.monitor.isMasterDegraded()
    assert len(node.requests) == 0


def check_checkpoint_finalize(nodes):
    for n in nodes:
        for checkpoint in n.master_replica.checkpoints.values():
            assert checkpoint.isStable
