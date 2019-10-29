import pytest

from plenum.test.delayers import req_delay, ppgDelay
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

from plenum.server.quorums import Quorum
from plenum.test.helper import sdk_send_random_and_check

from plenum.test.malicious_behaviors_node import dont_send_messages_to, dont_send_propagate
from plenum.test.spy_helpers import get_count


@pytest.fixture(scope='function')
def setup(txnPoolNodeSet):
    faulty_node = txnPoolNodeSet[-1]
    # Set quorum a bit more, so that faulty_node will request propagates
    faulty_node.quorums.propagate = Quorum(3)

    # do not receive requests and Propagates so that Propagates will be requested
    faulty_node.clientIbStasher.delay(req_delay())
    faulty_node.nodeIbStasher.delay(ppgDelay())
    return faulty_node


def test_node_request_only_needed_propagates(looper, setup, txnPoolNodeSet,
                                             sdk_wallet_client, sdk_pool_handle, tconf):
    """
    One of node lacks sufficient propagates
    """
    delay = tconf.PROPAGATE_REQUEST_DELAY
    faulty_node = setup

    old_count_recv_ppg = get_count(faulty_node, faulty_node.processPropagate)

    old_count_prop_req_alpha = get_count(txnPoolNodeSet[0], txnPoolNodeSet[0].process_message_req)
    old_count_prop_req_beta = get_count(txnPoolNodeSet[1], txnPoolNodeSet[1].process_message_req)
    old_count_prop_req_gamma = get_count(txnPoolNodeSet[2], txnPoolNodeSet[2].process_message_req)

    sent_reqs = 5
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              sent_reqs)
    looper.runFor(delay * 1.5)
    propagates_count = len(txnPoolNodeSet) - 1
    assert get_count(faulty_node, faulty_node.processPropagate) == old_count_recv_ppg + sent_reqs * propagates_count

    assert get_count(txnPoolNodeSet[0], txnPoolNodeSet[0].process_message_req) == old_count_prop_req_alpha + sent_reqs
    assert get_count(txnPoolNodeSet[1], txnPoolNodeSet[1].process_message_req) == old_count_prop_req_beta + sent_reqs
    assert get_count(txnPoolNodeSet[2], txnPoolNodeSet[2].process_message_req) == old_count_prop_req_gamma + sent_reqs

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
