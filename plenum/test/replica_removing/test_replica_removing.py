import sys

import pytest

from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import cDelay
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.replica_removing.helper import check_replica_removed
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_requests, sdk_get_replies, sdk_send_random_and_check, waitForViewChange
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected, \
    get_master_primary_node, get_last_master_non_primary_node

from plenum.test.checkpoints.conftest import chkFreqPatched

logger = getlogger()
CHK_FREQ = 2


@pytest.fixture(scope="function")
def view_change(txnPoolNodeSet, looper):
    # trigger view change on all nodes for return backup replicas
    do_view_change(txnPoolNodeSet, looper)
    for n in txnPoolNodeSet:
        assert n.requiredNumberOfInstances == n.replicas.num_replicas


def test_replica_removal(looper,
                         txnPoolNodeSet,
                         sdk_pool_handle,
                         sdk_wallet_client,
                         chkFreqPatched,
                         view_change):

    node = txnPoolNodeSet[0]
    start_replicas_count = node.replicas.num_replicas
    instance_id = start_replicas_count - 1

    node.replicas.remove_replica(instance_id)

    check_replica_removed(node, start_replicas_count, instance_id)


def test_replica_removal_does_not_cause_master_degradation(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        chkFreqPatched, view_change):

    node = txnPoolNodeSet[0]

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    node.replicas.remove_replica(node.replicas.num_replicas - 1)

    assert not node.monitor.isMasterDegraded()

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

    assert not node.monitor.isMasterDegraded()


def test_removed_replica_restored_on_view_change(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath, chkFreqPatched, view_change):
    """
    1. Remove replica on some node which is not master primary
    2. Reconnect the node which was master primary so far
    3. Check that nodes and replicas correctly added
    """
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    node = get_last_master_non_primary_node(txnPoolNodeSet)
    start_replicas_count = node.replicas.num_replicas
    instance_id = start_replicas_count - 1

    node.replicas.remove_replica(instance_id)
    check_replica_removed(node, start_replicas_count, instance_id)

    # trigger view change on all nodes
    master_primary = get_master_primary_node(txnPoolNodeSet)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, master_primary)
    txnPoolNodeSet.remove(master_primary)
    looper.removeProdable(master_primary)
    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)

    restarted_node = start_stopped_node(master_primary, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(restarted_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1,
                      customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    assert start_replicas_count == node.replicas.num_replicas


def test_ordered_request_freed_on_replica_removal(looper,
                                                  txnPoolNodeSet,
                                                  sdk_pool_handle,
                                                  sdk_wallet_client,
                                                  chkFreqPatched,
                                                  view_change):
    node = txnPoolNodeSet[0]
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    assert len(node.requests) == 1

    forwardedToBefore = next(iter(node.requests.values())).forwardedTo
    node.replicas.remove_replica(node.replicas.num_replicas - 1)

    assert len(node.requests) == 1
    forwardedToAfter = next(iter(node.requests.values())).forwardedTo
    assert forwardedToAfter == forwardedToBefore - 1
    chkChkpoints(txnPoolNodeSet, 1)

    # Send one more request to stabilize checkpoint
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0))
    assert len(node.requests) == 0


def test_unordered_request_freed_on_replica_removal(looper,
                                                    txnPoolNodeSet,
                                                    sdk_pool_handle,
                                                    sdk_wallet_client,
                                                    chkFreqPatched,
                                                    view_change):
    node = txnPoolNodeSet[0]
    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    with delay_rules(stashers, cDelay(delay=sys.maxsize)):
        req = sdk_send_random_requests(looper,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       1)
        looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                      waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                      waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                      waits.expectedCommittedTime(len(txnPoolNodeSet)))

        assert len(node.requests) == 1

        forwardedToBefore = next(iter(node.requests.values())).forwardedTo
        node.replicas.remove_replica(node.replicas.num_replicas - 1)

        assert len(node.requests) == 1
        forwardedToAfter = next(iter(node.requests.values())).forwardedTo
        assert forwardedToAfter == forwardedToBefore - 1
        chkChkpoints(txnPoolNodeSet, 0)

    sdk_get_replies(looper, req)
    chkChkpoints(txnPoolNodeSet, 1)

    # Send one more request to stabilize checkpoint
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0))
    assert len(node.requests) == 0


def do_view_change(txnPoolNodeSet, looper):
    # trigger view change on all nodes
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)