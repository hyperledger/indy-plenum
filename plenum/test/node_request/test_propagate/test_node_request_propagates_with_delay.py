import pytest
from plenum.test.helper import sdk_send_random_and_check

from plenum.test.delayers import ppgDelay, req_delay
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import getNonPrimaryReplicas


@pytest.fixture(scope='function')
def setup(txnPoolNodeSet):
    faulty_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[1].node
    faulty_node.nodeIbStasher.delay(ppgDelay())
    faulty_node.clientIbStasher.delay(req_delay())
    return faulty_node


def test_node_request_propagates_with_delay(looper, setup, txnPoolNodeSet,
                                            sdk_wallet_client, sdk_pool_handle, tconf):
    """
    One of node lacks sufficient propagates
    """
    delay = tconf.PROPAGATE_REQUEST_DELAY
    faulty_node = setup

    old_count_recv_ppg = get_count(faulty_node, faulty_node.processPropagate)

    sent_reqs = 5
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              sent_reqs)

    looper.runFor(delay / 4)
    assert get_count(faulty_node, faulty_node.processPropagate) == old_count_recv_ppg

    # "delay / 4" let us avoid intermittent cases
    looper.runFor(delay / 2 + delay / 4)
    propagates_count = len(txnPoolNodeSet) - 1
    assert get_count(faulty_node, faulty_node.processPropagate) == old_count_recv_ppg + sent_reqs * propagates_count
