import pytest

from plenum.test.delayers import ppDelay
from plenum.test.helper import sdk_send_random_requests, assertExp
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

REQ_COUNT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    old_b_size = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    yield tconf

    tconf.Max3PCBatchSize = old_b_size


def test_backup_stabilized_checkpoint_on_view_change(looper,
                                                     txnPoolNodeSet,
                                                     sdk_wallet_client,
                                                     sdk_pool_handle):
    # Delta:1
    backup = txnPoolNodeSet[-1].replicas[1]
    count_of_replicas = len(txnPoolNodeSet[0].replicas)
    with delay_rules([n.nodeIbStasher for n in txnPoolNodeSet], ppDelay(instId=0)):
        sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, REQ_COUNT)
        looper.run(eventually(lambda r: assertExp(r.last_ordered_3pc == (0, REQ_COUNT)), backup))
        # assert that all of requests are propagated
        for n in txnPoolNodeSet:
            for req in n.requests.values():
                assert req.forwardedTo == count_of_replicas

        ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    # check, that all requests have been freed on backups
    for n in txnPoolNodeSet:
        for req in n.requests.values():
            assert req.forwardedTo == count_of_replicas - 1