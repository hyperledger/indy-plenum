import pytest

from plenum.test.helper import max_3pc_batch_limits, sdk_send_random_requests, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from stp_core.common.log import getlogger

BATCHES_TO_ORDER = 20
MAX_BATCHES_IN_FLIGHT = 4

logger = getlogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        old = tconf.Max3PCBatchesInFlight
        tconf.Max3PCBatchesInFlight = MAX_BATCHES_IN_FLIGHT
        yield tconf
        tconf.Max3PCBatchesInFlight = old


def test_max_3pc_batches_in_flight(tdir, tconf,
                                   looper,
                                   txnPoolNodeSet,
                                   sdk_pool_handle,
                                   sdk_wallet_client):
    # Check pool initial state
    initial_3pc = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    for node in txnPoolNodeSet[1:]:
        assert node.master_replica.last_ordered_3pc == initial_3pc

    # Utility
    def has_requests_to_order():
        for node in txnPoolNodeSet:
            if node.master_replica.last_ordered_3pc[1] < initial_3pc[1] + BATCHES_TO_ORDER:
                return True
        return False

    # Send a number of requests
    reqs = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, BATCHES_TO_ORDER)

    # Continuously check number of batches in flight
    while has_requests_to_order():
        logger.info("Next cycle")
        for node in txnPoolNodeSet:
            for replica in node.replicas.values():
                batches_in_flight = replica.lastPrePrepareSeqNo - replica.last_ordered_3pc[1]
                assert batches_in_flight <= MAX_BATCHES_IN_FLIGHT
        looper.runFor(0.01)

    # Check all requests are ordered
    sdk_get_and_check_replies(looper, reqs)

    # Ensure that all nodes will eventually have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
