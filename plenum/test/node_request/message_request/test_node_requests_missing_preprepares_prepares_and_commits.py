import pytest

from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.ordering_service import OrderingService
from plenum.test import waits
from plenum.test.delayers import delay_3pc
from plenum.test.node_request.message_request.helper import \
    check_pp_out_of_sync
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from plenum.test.waits import expectedPoolGetReadyTimeout
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, reconnect_node_and_ensure_connected
from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check, assertEquality
from stp_core.loop.eventually import eventually

logger = getlogger()

nodeCount = 4


@pytest.fixture(scope="module")
def tconf(tconf):
    oldMax3PCBatchSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 5
    yield tconf
    tconf.Max3PCBatchSize = oldMax3PCBatchSize


def test_node_requests_missing_preprepares_prepares_and_commits(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle,
        tdir, allPluginsPath):
    """
    1 of 4 nodes goes down ((simulate this by dropping requests)). A new request comes in and is ordered by
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
    disconnected_node_stashers = disconnected_node.nodeIbStasher

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              INIT_REQS_CNT)
    init_ledger_size = txnPoolNodeSet[0].domainLedger.size

    with delay_rules_without_processing(disconnected_node_stashers, delay_3pc()):
        last_ordered_key = txnPoolNodeSet[0].master_replica.last_ordered_3pc
        sdk_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  MISSING_REQS_CNT)
        looper.run(eventually(check_pp_out_of_sync,
                              alive_nodes,
                              [disconnected_node],
                              last_ordered_key,
                              retryWait=1,
                              timeout=expectedPoolGetReadyTimeout(len(txnPoolNodeSet))))

    for node in alive_nodes:
        assert node.domainLedger.size == init_ledger_size + MISSING_REQS_CNT
    # Ensure that the reconnected node has not caught up though
    assert disconnected_node.domainLedger.size == init_ledger_size

    ordering_service = disconnected_node.master_replica._ordering_service
    assert ordering_service.spylog.count(OrderingService._request_pre_prepare) == 0
    assert ordering_service.spylog.count(OrderingService._request_prepare) == 0
    assert ordering_service.spylog.count(OrderingService._request_commit) == 0
    assert disconnected_node.master_replica._message_req_service.spylog.count(
        MessageReqService.process_message_rep) == 0
    doOrderTimesBefore = ordering_service.spylog.count(OrderingService._do_order)

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              REQS_AFTER_RECONNECT_CNT)
    waitNodeDataEquality(looper, disconnected_node, *alive_nodes)

    assert ordering_service.spylog.count(OrderingService._request_pre_prepare) > 0
    assert ordering_service.spylog.count(OrderingService._request_prepare) > 0
    assert ordering_service.spylog.count(OrderingService._request_commit) > 0
    assert disconnected_node.master_replica._message_req_service.spylog.count(
        MessageReqService.process_message_rep) > 0
    doOrderTimesAfter = ordering_service.spylog.count(OrderingService._do_order)
    # Ensure that the reconnected node has ordered both the missed 3PC-batch and the new 3PC-batch
    assert doOrderTimesAfter - doOrderTimesBefore == 2

    for node in txnPoolNodeSet:
        assert node.domainLedger.size == (init_ledger_size +
                                          MISSING_REQS_CNT +
                                          REQS_AFTER_RECONNECT_CNT)

    def check_all_ordered():
        for node in txnPoolNodeSet:
            assert node.domainLedger.size == (init_ledger_size +
                                              MISSING_REQS_CNT +
                                              REQS_AFTER_RECONNECT_CNT)

    looper.run(eventually(check_all_ordered, timeout=20))
