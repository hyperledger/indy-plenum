from plenum.server.consensus.message_request.message_req_service import MessageReqService
from plenum.server.consensus.ordering_service import OrderingService
from plenum.test.delayers import delay_3pc
from plenum.test.node_request.message_request.helper import \
    check_pp_out_of_sync
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.waits import expectedPoolGetReadyTimeout
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check
from stp_core.loop.eventually import eventually

logger = getlogger()

nodeCount = 4


def test_node_requests_missing_preprepares_and_prepares(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle,
        tconf, tdir, allPluginsPath):
    """
    2 of 4 nodes go down (simulate this by dropping requests), so pool can not process any more incoming requests.
    A new request comes in. After a while those 2 nodes come back alive.
    Another request comes in. Check that previously disconnected two nodes
    request missing PREPREPARES and PREPARES and the pool successfully handles
    both transactions after that.
    """
    INIT_REQS_CNT = 5
    MISSING_REQS_CNT = 4
    REQS_AFTER_RECONNECT_CNT = 1

    disconnected_nodes = txnPoolNodeSet[2:]
    alive_nodes = txnPoolNodeSet[:2]
    disconnected_nodes_stashers = [n.nodeIbStasher for n in disconnected_nodes]

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              INIT_REQS_CNT)
    init_ledger_size = txnPoolNodeSet[0].domainLedger.size

    with delay_rules_without_processing(disconnected_nodes_stashers, delay_3pc()):
        sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, MISSING_REQS_CNT)
        last_ordered_key = txnPoolNodeSet[0].master_replica.last_ordered_3pc
        looper.run(eventually(check_pp_out_of_sync,
                              alive_nodes,
                              disconnected_nodes,
                              last_ordered_key,
                              retryWait=1,
                              timeout=expectedPoolGetReadyTimeout(len(txnPoolNodeSet))))

    for node in txnPoolNodeSet:
        assert node.domainLedger.size == init_ledger_size

    for node in disconnected_nodes:
        assert node.master_replica._ordering_service.spylog.count(OrderingService._request_pre_prepare) == 0
        assert node.master_replica._ordering_service.spylog.count(OrderingService._request_prepare) == 0
        assert node.master_replica._message_req_service.spylog.count(MessageReqService.process_message_rep) == 0

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              REQS_AFTER_RECONNECT_CNT)
    waitNodeDataEquality(looper, disconnected_nodes[0], *txnPoolNodeSet[:-1])

    for node in disconnected_nodes:
        assert node.master_replica._ordering_service.spylog.count(OrderingService._request_pre_prepare) > 0
        assert node.master_replica._ordering_service.spylog.count(OrderingService._request_prepare) > 0
        assert node.master_replica._message_req_service.spylog.count(MessageReqService.process_message_rep) > 0

    def check_all_ordered():
        for node in txnPoolNodeSet:
            assert node.domainLedger.size == (init_ledger_size +
                                              MISSING_REQS_CNT +
                                              REQS_AFTER_RECONNECT_CNT)

    looper.run(eventually(check_all_ordered, timeout=20))
