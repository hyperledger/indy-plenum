import sys

import pytest

from plenum.common.request import Request
from plenum.test.delayers import cDelay
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_requests, sdk_get_replies, sdk_send_random_and_check, waitForViewChange
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected

from plenum.test.checkpoints.conftest import chkFreqPatched

logger = getlogger()
CHK_FREQ = 1


@pytest.fixture(scope="function")
def view_change(txnPoolNodeSet, looper):
    # trigger view change on all nodes for return backup replicas
    do_view_change(txnPoolNodeSet, looper)
    for n in txnPoolNodeSet:
        assert n.requiredNumberOfInstances == n.replicas.num_replicas


def test_replica_removing(looper,
                          txnPoolNodeSet,
                          sdk_pool_handle,
                          sdk_wallet_client,
                          view_change):
    """
    1. Remove replica
    2. Ordering
    3. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    instance_id = start_replicas_count - 1
    node.replicas.remove_replica(instance_id)
    _check_replica_removed(node, start_replicas_count, instance_id)
    assert not node.monitor.isMasterDegraded()
    assert len(node.requests) == 0


def test_replica_removing_before_vc_with_primary_disconnected(looper,
                                                              txnPoolNodeSet,
                                                              sdk_pool_handle,
                                                              sdk_wallet_client,
                                                              tconf,
                                                              tdir,
                                                              allPluginsPath,
                                                              chkFreqPatched,
                                                              view_change):
    """
    1. Remove replica
    2. Reconnect master primary
    3. Check that nodes and replicas correctly added
    """
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    instance_id = start_replicas_count - 1
    node.replicas.remove_replica(instance_id)
    _check_replica_removed(node, start_replicas_count, instance_id)
    assert not node.monitor.isMasterDegraded()
    assert len(node.requests) == 0
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
    assert start_replicas_count == node.replicas.num_replicas


def test_replica_removing_before_ordering(looper,
                                          txnPoolNodeSet,
                                          sdk_pool_handle,
                                          sdk_wallet_client,
                                          chkFreqPatched,
                                          view_change):
    """
    1. Remove replica
    2. Ordering
    3. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    instance_id = start_replicas_count - 1
    node.replicas.remove_replica(instance_id)
    _check_replica_removed(node, start_replicas_count, instance_id)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    looper.run(eventually(check_checkpoint_finalize, txnPoolNodeSet))
    assert not node.monitor.isMasterDegraded()
    assert len(node.requests) == 0


def test_replica_removing_in_ordering(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      chkFreqPatched,
                                      view_change):
    """
    1. Start ordering (send pre-prepares on backup)
    2. Remove replica
    3. Finish ordering
    4. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    instance_id = start_replicas_count - 1
    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]
    with delay_rules(stashers, cDelay(delay=sys.maxsize)):
        req = sdk_send_random_requests(looper,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       1)

        def chk():
            assert len(node.requests) > 0
        looper.run(eventually(chk))
        digest = Request(**req[0][0]).digest
        old_forwarded_to = node.requests[digest].forwardedTo
        node.replicas.remove_replica(instance_id)
        assert old_forwarded_to - 1 == node.requests[digest].forwardedTo
    sdk_get_replies(looper, req)
    looper.run(eventually(check_checkpoint_finalize, txnPoolNodeSet))
    _check_replica_removed(node, start_replicas_count, instance_id)
    assert not node.monitor.isMasterDegraded()
    assert len(node.requests) == 0


def test_replica_removing_after_ordering(looper,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         view_change,
                                         chkFreqPatched):
    """
    1. Ordering
    2. Remove replica
    3. Check monitor and replicas count
    """
    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    looper.run(eventually(check_checkpoint_finalize, txnPoolNodeSet))
    instance_id = start_replicas_count - 1
    node.replicas.remove_replica(instance_id)
    _check_replica_removed(node, start_replicas_count, instance_id)
    assert not node.monitor.isMasterDegraded()
    assert len(node.requests) == 0


def _check_replica_removed(node, start_replicas_count, instance_id):
    replicas_count = start_replicas_count - 1
    assert node.replicas.num_replicas == replicas_count
    replicas_lists = [node.replicas.keys(),
                      node.replicas._messages_to_replicas.keys(),
                      node.monitor.numOrderedRequests.keys(),
                      node.monitor.clientAvgReqLatencies.keys(),
                      node.monitor.throughputs.keys(),
                      node.monitor.requestTracker.instances_ids,
                      node.monitor.instances.ids]
    assert all(instance_id not in replicas for replicas in replicas_lists)
    if node.monitor.acc_monitor is not None:
        assert node.monitor.acc_monitor == replicas_count
        assert instance_id not in node.replicas.keys()


def check_checkpoint_finalize(nodes):
    for n in nodes:
        for checkpoint in n.master_replica.checkpoints.values():
            assert checkpoint.isStable


def do_view_change(txnPoolNodeSet, looper):
    # trigger view change on all nodes
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)