import pytest
import time

from plenum.server.replica import Replica
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.message_request.helper import \
    check_pp_out_of_sync
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.waits import expectedPoolGetReadyTimeout
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check

logger = getlogger()

nodeCount = 4


def test_node_requests_missing_preprepares_and_prepares_after_long_disconnection(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle,
        tconf, tdirWithPoolTxns, allPluginsPath):
    """
    2 of 4 nodes go down, so pool can not process any more incoming requests.
    A new request comes in.
    Test than waits for some time to ensure that PrePrepare was created
    long enough seconds to be dropped by time checker.
    Two stopped nodes come back alive.
    Another request comes in.
    Check that previously disconnected two nodes request missing PREPREPARES
    and PREPARES and the pool successfully handles both transactions.
    """
    INIT_REQS_CNT = 5
    MISSING_REQS_CNT = 4
    REQS_AFTER_RECONNECT_CNT = 1
    alive_nodes = []
    disconnected_nodes = []

    for node in txnPoolNodeSet:
        if node.hasPrimary:
            alive_nodes.append(node)
        else:
            disconnected_nodes.append(node)

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              INIT_REQS_CNT)

    waitNodeDataEquality(looper, disconnected_nodes[0], *txnPoolNodeSet)
    init_ledger_size = txnPoolNodeSet[0].domainLedger.size

    current_node_set = set(txnPoolNodeSet)
    for node in disconnected_nodes:
        disconnect_node_and_ensure_disconnected(looper,
                                                current_node_set,
                                                node,
                                                stopNode=False)
        current_node_set.remove(node)

    sdk_send_random_requests(looper,
                             sdk_pool_handle,
                             sdk_wallet_client,
                             MISSING_REQS_CNT)

    looper.run(eventually(check_pp_out_of_sync,
                          alive_nodes,
                          disconnected_nodes,
                          retryWait=1,
                          timeout=expectedPoolGetReadyTimeout(len(txnPoolNodeSet))))

    preprepare_deviation = 4
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = preprepare_deviation
    time.sleep(preprepare_deviation * 2)

    for node in disconnected_nodes:
        current_node_set.add(node)
        reconnect_node_and_ensure_connected(looper, current_node_set, node)

    for node in txnPoolNodeSet:
        assert node.domainLedger.size == init_ledger_size

    for node in disconnected_nodes:
        assert node.master_replica.spylog.count(Replica._request_pre_prepare) == 0
        assert node.master_replica.spylog.count(Replica._request_prepare) == 0
        assert node.master_replica.spylog.count(Replica.process_requested_pre_prepare) == 0
        assert node.master_replica.spylog.count(Replica.process_requested_prepare) == 0

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              REQS_AFTER_RECONNECT_CNT)

    waitNodeDataEquality(looper, disconnected_nodes[0], *txnPoolNodeSet)

    for node in disconnected_nodes:
        assert node.master_replica.spylog.count(Replica._request_pre_prepare) > 0
        assert node.master_replica.spylog.count(Replica._request_prepare) > 0
        assert node.master_replica.spylog.count(Replica.process_requested_pre_prepare) > 0
        assert node.master_replica.spylog.count(Replica.process_requested_prepare) > 0

    for node in txnPoolNodeSet:
        assert node.domainLedger.size == (init_ledger_size +
                                          MISSING_REQS_CNT +
                                          REQS_AFTER_RECONNECT_CNT)


@pytest.yield_fixture(autouse=True)
def teardown(tconf):
    original_deviation = tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    yield
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = original_deviation
