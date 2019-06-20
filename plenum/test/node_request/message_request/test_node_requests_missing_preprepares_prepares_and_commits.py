import pytest

from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.node_request.message_request.helper import \
    check_pp_out_of_sync
from plenum.test.waits import expectedPoolGetReadyTimeout
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, reconnect_node_and_ensure_connected
from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check
from stp_core.loop.eventually import eventually

logger = getlogger()

nodeCount = 4


@pytest.fixture(scope="module")
def tconf(tconf):
    oldMax3PCBatchSize = tconf.Max3PCBatchSize
    oldMax3PCBatchWait = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = 5
    tconf.Max3PCBatchWait = 2
    yield tconf

    tconf.Max3PCBatchSize = oldMax3PCBatchSize
    tconf.Max3PCBatchWait = oldMax3PCBatchWait


def test_node_requests_missing_preprepares_prepares_and_commits(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    """
    1 of 4 nodes goes down. A new request comes in and is ordered by
    the 3 remaining nodes. After a while the previously disconnected node
    comes back alive. Another request comes in. Check that the previously
    disconnected node requests missing PREPREPARES, PREPARES and COMMITS,
    orders the previous request and all the nodes successfully handles
    the last request.
    """
    INIT_REQS_CNT = 5
    MISSING_REQS_CNT = 4
    REQS_AFTER_RECONNECT_CNT = 1
    disconnected_node = txnPoolNodeSet[3]
    alive_nodes = txnPoolNodeSet[:3]

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              INIT_REQS_CNT)
    init_ledger_size = txnPoolNodeSet[0].domainLedger.size

    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet,
                                            disconnected_node, stopNode=False)

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              MISSING_REQS_CNT)

    looper.run(eventually(check_pp_out_of_sync,
                          alive_nodes,
                          [disconnected_node],
                          retryWait=1,
                          timeout=expectedPoolGetReadyTimeout(len(txnPoolNodeSet))))

    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, disconnected_node)
    # Give time for the reconnected node to catch up if it is going to do it
    looper.runFor(waits.expectedPoolConsistencyProof(len(txnPoolNodeSet)) +
                  waits.expectedPoolCatchupTime(len(txnPoolNodeSet)))

    for node in alive_nodes:
        assert node.domainLedger.size == init_ledger_size + MISSING_REQS_CNT
    # Ensure that the reconnected node has not caught up though
    assert disconnected_node.domainLedger.size == init_ledger_size

    assert disconnected_node.master_replica.spylog.count(Replica._request_pre_prepare) == 0
    assert disconnected_node.master_replica.spylog.count(Replica._request_prepare) == 0
    assert disconnected_node.master_replica.spylog.count(Replica._request_commit) == 0
    assert disconnected_node.master_replica.spylog.count(Replica.process_requested_pre_prepare) == 0
    assert disconnected_node.master_replica.spylog.count(Replica.process_requested_prepare) == 0
    assert disconnected_node.master_replica.spylog.count(Replica.process_requested_commit) == 0
    doOrderTimesBefore = disconnected_node.master_replica.spylog.count(Replica.doOrder)

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              REQS_AFTER_RECONNECT_CNT)
    waitNodeDataEquality(looper, disconnected_node, *alive_nodes)

    assert disconnected_node.master_replica.spylog.count(Replica._request_pre_prepare) > 0
    assert disconnected_node.master_replica.spylog.count(Replica._request_prepare) > 0
    assert disconnected_node.master_replica.spylog.count(Replica._request_commit) > 0
    assert disconnected_node.master_replica.spylog.count(Replica.process_requested_pre_prepare) > 0
    assert disconnected_node.master_replica.spylog.count(Replica.process_requested_prepare) > 0
    assert disconnected_node.master_replica.spylog.count(Replica.process_requested_commit) > 0
    doOrderTimesAfter = disconnected_node.master_replica.spylog.count(Replica.doOrder)
    # Ensure that the reconnected node has ordered both the missed 3PC-batch and the new 3PC-batch
    assert doOrderTimesAfter - doOrderTimesBefore == 2

    for node in txnPoolNodeSet:
        assert node.domainLedger.size == (init_ledger_size +
                                          MISSING_REQS_CNT +
                                          REQS_AFTER_RECONNECT_CNT)
