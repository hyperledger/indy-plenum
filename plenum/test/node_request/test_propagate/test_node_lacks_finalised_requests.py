import pytest
from plenum.test.delayers import ppgDelay, req_delay
from plenum.test.helper import send_reqs_to_nodes_and_verify_all_replies
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional
from plenum.test.spy_helpers import get_count, getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas


@pytest.fixture(scope='function', params=['client_requests',
                                          'no_client_requests'])
def setup(request, txnPoolNodeSet):
    # Test once when client request is received and once when not received

    # Choosing a faulty node which is primary in neither instance, this helps
    # in the that same PROPAGATEs are not requested again by the node
    faulty_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[1].node
    if request.param == 'client_requests':
        # Long delay in PROPAGATEs
        faulty_node.nodeIbStasher.delay(ppgDelay(90))
        return faulty_node, True
    if request.param == 'no_client_requests':
        # Long delay in PROPAGATEs
        faulty_node.nodeIbStasher.delay(ppgDelay(90))
        # Long delay in Client Requests
        faulty_node.clientIbStasher.delay(req_delay(90))
        return faulty_node, False


def test_node_request_propagates(looper, setup, txnPoolNodeSet, client1,
                                 wallet1, client1Connected, request):
    """
    One of node lacks sufficient propagates
    """
    faulty_node, recv_client_requests = setup

    old_count_recv_ppg = get_count(faulty_node, faulty_node.processPropagate)
    old_count_recv_req = get_count(faulty_node, faulty_node.processRequest)
    old_count_request_propagates = get_count(
        faulty_node, faulty_node.request_propagates)

    sent_reqs = 5
    send_reqs_to_nodes_and_verify_all_replies(
        looper, wallet1, client1, sent_reqs)

    assert get_count(
        faulty_node, faulty_node.processPropagate) > old_count_recv_ppg
    if recv_client_requests:
        assert get_count(
            faulty_node, faulty_node.processRequest) > old_count_recv_req
    else:
        assert get_count(
            faulty_node, faulty_node.processRequest) == old_count_recv_req

    # Attempt to request PROPAGATEs was made twice, since the faulty node has
    # 2 replicas
    assert get_count(faulty_node, faulty_node.request_propagates) - \
        old_count_request_propagates == 2

    requested_propagate_counts = getAllReturnVals(
        faulty_node, faulty_node.request_propagates)

    # The last attempt to request PROPAGATEs was not successful
    assert requested_propagate_counts[0] == 0
    # The first attempt to request PROPAGATEs was successful as PROPAGATEs
    # were requested for all nodes
    assert requested_propagate_counts[1] == sent_reqs

    faulty_node.nodeIbStasher.reset_delays_and_process_delayeds()
    ensure_pool_functional(looper, txnPoolNodeSet, wallet1, client1, 4)
