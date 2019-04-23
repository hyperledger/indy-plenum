import pytest

from plenum.test.delayers import delay_3pc
from plenum.test.helper import max_3pc_batch_limits, sdk_send_random_requests, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import start_delaying, stop_delaying_and_process
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

BATCHES_TO_ORDER = 10
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
    def check_ordered_till(pp_seq_no: int):
        for node in txnPoolNodeSet:
            last_ordered = node.master_replica.last_ordered_3pc
            assert last_ordered[0] == initial_3pc[0]
            assert last_ordered[1] == pp_seq_no

    # Delay some commits
    all_stashers = [node.nodeIbStasher for node in txnPoolNodeSet]
    delayers = []
    for num in range(BATCHES_TO_ORDER):
        pp_seq_no = initial_3pc[1] + num + 1
        delayer = start_delaying(all_stashers, delay_3pc(after=pp_seq_no - 1,
                                                         before=pp_seq_no + 1))
        delayers.append((pp_seq_no, delayer))

    # Send a number of requests
    reqs = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, BATCHES_TO_ORDER)

    # Continuously check number of batches in flight
    for pp_seq_no, delayer in delayers:
        stop_delaying_and_process(delayer)
        looper.run(eventually(check_ordered_till, pp_seq_no))

        for node in txnPoolNodeSet:
            for replica in node.replicas.values():
                batches_in_flight = replica.lastPrePrepareSeqNo - replica.last_ordered_3pc[1]
                assert batches_in_flight <= MAX_BATCHES_IN_FLIGHT

    # Check all requests are ordered
    sdk_get_and_check_replies(looper, reqs)

    # Ensure that all nodes will eventually have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
