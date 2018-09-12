import pytest

from plenum.common.util import compare_3PC_keys
from plenum.test.checkpoints.conftest import tconf, chkFreqPatched, \
    reqs_for_checkpoint
from plenum.test.delayers import pDelay, cDelay, ppDelay
from plenum.test.node_catchup.test_node_reject_invalid_txn_during_catchup import \
    get_any_non_primary_node
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import \
    waitNodeDataEquality, \
    waitNodeDataInequality

from stp_core.loop.eventually import eventually


logger = getlogger()


def make_master_replica_lag(node):
    node.nodeIbStasher.delay(ppDelay(1200, 0))
    node.nodeIbStasher.delay(pDelay(1200, 0))
    node.nodeIbStasher.delay(cDelay(1200, 0))


def compare_last_ordered_3pc(node):
    last_ordered_by_master = node.replicas._master_replica.last_ordered_3pc
    comparison_results = {
        compare_3PC_keys(replica.last_ordered_3pc, last_ordered_by_master)
        for replica in node.replicas.values() if not replica.isMaster
    }
    assert len(comparison_results) == 1
    return comparison_results.pop()


def backup_replicas_run_forward(node):
    assert compare_last_ordered_3pc(node) < 0


def replicas_synced(node):
    assert compare_last_ordered_3pc(node) == 0


CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_node_catchup_causes_no_desync(looper, txnPoolNodeSet, sdk_pool_handle,
                                       sdk_wallet_client, monkeypatch,
                                       chkFreqPatched, reqs_for_checkpoint):
    """
    Checks that transactions received by catchup do not
    break performance monitoring
    """

    max_batch_size = chkFreqPatched.Max3PCBatchSize
    lagging_node = get_any_non_primary_node(txnPoolNodeSet)
    rest_nodes = set(txnPoolNodeSet).difference({lagging_node})

    # Make master replica lagging by hiding all messages sent to it
    make_master_replica_lag(lagging_node)
    monkeypatch.setattr(lagging_node.master_replica,
                        '_request_missing_three_phase_messages',
                        lambda *x, **y: None)

    # Send some requests and check that all replicas except master executed it
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              reqs_for_checkpoint - max_batch_size)
    waitNodeDataInequality(looper, lagging_node, *rest_nodes)
    looper.run(eventually(backup_replicas_run_forward, lagging_node))

    assert not lagging_node.monitor.isMasterDegraded()

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              reqs_for_checkpoint + max_batch_size)
    # Check that catchup done
    waitNodeDataEquality(looper, lagging_node, *rest_nodes)

    lagging_node.reset_delays_and_process_delayeds()

    # Send some more requests to ensure that backup and master replicas
    # are in the same state
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              reqs_for_checkpoint - max_batch_size)
    looper.run(eventually(replicas_synced, lagging_node))

    # Check that master is not considered to be degraded
    assert not lagging_node.monitor.isMasterDegraded()
