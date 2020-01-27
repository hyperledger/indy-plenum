import sys

import pytest

from plenum.common.constants import COMMIT, SEQ_NO_DB_LABEL
from plenum.test import waits
from plenum.test.checkpoints.helper import check_for_nodes, check_stable_checkpoint
from plenum.test.delayers import cDelay, msg_rep_delay
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_catchup.test_config_ledger import start_stopped_node
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.replica_removing.helper import check_replica_removed
from plenum.test.stasher import delay_rules
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_requests, sdk_get_replies, sdk_send_random_and_check, waitForViewChange, \
    freshness, sdk_send_batches_of_random_and_check, get_pp_seq_no, assertExp
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected, \
    get_master_primary_node, get_last_master_non_primary_node

from plenum.test.checkpoints.conftest import chkFreqPatched

logger = getlogger()
CHK_FREQ = 3


@pytest.fixture(scope="module")
def tconf(tconf):
    max_b_size = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = max_b_size


@pytest.fixture(scope="function")
def view_change(txnPoolNodeSet, looper):
    # trigger view change on all nodes for return backup replicas
    do_view_change(txnPoolNodeSet, looper)
    for n in txnPoolNodeSet:
        assert n.requiredNumberOfInstances == n.replicas.num_replicas


def get_forwarded_to_all(node, is_ordered=False):
    for digest, req_state in node.requests.items():
        if req_state.forwardedTo == node.replicas.num_replicas:
            if is_ordered and \
                node.db_manager.get_store(SEQ_NO_DB_LABEL).get_by_payload_digest(req_state.request.payload_digest) == (None, None):
                continue
            return (digest, req_state)
    return (None, None)


def test_primary_after_replica_restored(looper,
                                        txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_client,
                                        chkFreqPatched,
                                        view_change):
    A, B, C, D = txnPoolNodeSet
    assert B.master_replica.isPrimary
    assert C.replicas._replicas[1].isPrimary

    D.replicas.remove_replica(1)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2 * CHK_FREQ)
    do_view_change(txnPoolNodeSet, looper)
    batches_before = D.replicas._replicas[1].last_ordered_3pc[1]
    assert D.replicas._replicas[1].isPrimary

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2 * CHK_FREQ)
    batches_after = D.replicas._replicas[1].last_ordered_3pc[1]
    assert batches_after > batches_before


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
                      customTimeout=2 * tconf.NEW_VIEW_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    assert start_replicas_count == node.replicas.num_replicas
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


def test_ordered_request_freed_on_replica_removal(looper,
                                                  txnPoolNodeSet,
                                                  sdk_pool_handle,
                                                  sdk_wallet_client,
                                                  chkFreqPatched,
                                                  view_change):
    node = txnPoolNodeSet[0]
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2)
    old_stable_checkpoint = node.master_replica._consensus_data.stable_checkpoint

    with delay_rules(node.nodeIbStasher, cDelay(), msg_rep_delay(types_to_delay=[COMMIT])):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

        f_d, f_r = get_forwarded_to_all(node)
        assert f_d
        node.replicas.remove_replica(node.replicas.num_replicas - 1)

        assert node.requests[f_d].forwardedTo == node.replicas.num_replicas
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, old_stable_checkpoint))
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, exclude_from_check=['check_primaries'])

    # Send one more request to stabilize checkpoint
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              CHK_FREQ - 1)
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, old_stable_checkpoint + CHK_FREQ))


def test_unordered_request_freed_on_replica_removal(looper,
                                                    txnPoolNodeSet,
                                                    sdk_pool_handle,
                                                    sdk_wallet_client,
                                                    chkFreqPatched,
                                                    view_change):
    node = txnPoolNodeSet[0]
    # Stabilize checkpoint
    # Send one more request to stabilize checkpoint
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              CHK_FREQ - get_pp_seq_no(txnPoolNodeSet) % CHK_FREQ)
    old_stable_checkpoint = node.master_replica._consensus_data.stable_checkpoint
    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    with delay_rules(stashers, cDelay(delay=sys.maxsize), msg_rep_delay(types_to_delay=[COMMIT])):
        req = sdk_send_random_requests(looper,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       1)
        looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                      waits.expectedPrePrepareTime(len(txnPoolNodeSet)) +
                      waits.expectedPrepareTime(len(txnPoolNodeSet)) +
                      waits.expectedCommittedTime(len(txnPoolNodeSet)))

        f_d, f_r = get_forwarded_to_all(node)
        assert f_d
        node.replicas.remove_replica(node.replicas.num_replicas - 1)

        assert node.requests[f_d].forwardedTo == node.replicas.num_replicas
        check_for_nodes(txnPoolNodeSet, check_stable_checkpoint, old_stable_checkpoint)

    sdk_get_replies(looper, req)
    check_for_nodes(txnPoolNodeSet, check_stable_checkpoint, old_stable_checkpoint)

    # Send one more request to stabilize checkpoint
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, CHK_FREQ - 1)

    looper.run(eventually(check_for_nodes,
                          txnPoolNodeSet,
                          check_stable_checkpoint,
                          old_stable_checkpoint + CHK_FREQ))
    assert len(node.requests) == 0


def do_view_change(txnPoolNodeSet, looper):
    # trigger view change on all nodes
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)