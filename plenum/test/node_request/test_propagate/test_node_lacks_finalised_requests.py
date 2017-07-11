from plenum.test.delayers import ppgDelay, req_delay
from plenum.test.helper import send_reqs_to_nodes_and_verify_all_replies
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import getNonPrimaryReplicas


def test_node_lacks_propagates(looper, txnPoolNodeSet, client1, wallet1,
                               client1Connected):
    """
    One of node lacks sufficient propagates
    """
    faulty_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[0].node
    other_nodes = [n for n in txnPoolNodeSet if n != faulty_node]

    # Long delay in PROPAGATEs
    faulty_node.nodeIbStasher.delay(ppgDelay(90))
    old_count = get_count(faulty_node, faulty_node.processPropagate)

    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 5)

    assert get_count(faulty_node, faulty_node.processPropagate) > old_count
    faulty_node.nodeIbStasher.reset_delays_and_process_delayeds()


def test_node_client_request_and_propagates(looper, txnPoolNodeSet, client1,
                                            wallet1, client1Connected):
    """
    One of node lacks sufficient propagates
    """
    faulty_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[0].node
    other_nodes = [n for n in txnPoolNodeSet if n != faulty_node]

    # Long delay in PROPAGATEs
    faulty_node.nodeIbStasher.delay(ppgDelay(90))
    # Long delay in Client Requests
    faulty_node.clientIbStasher.delay(req_delay(90))

    old_ppg_count = get_count(faulty_node, faulty_node.processPropagate)
    old_req_count = get_count(faulty_node, faulty_node.processRequest)

    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1, 5)

    assert get_count(faulty_node, faulty_node.processPropagate) > old_ppg_count
    assert get_count(faulty_node, faulty_node.processRequest) == old_req_count
