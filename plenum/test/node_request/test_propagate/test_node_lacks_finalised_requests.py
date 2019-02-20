import pytest
from plenum.test.delayers import ppgDelay, req_delay
from plenum.test.node_request.test_propagate.helper import sum_of_request_propagates
from plenum.test.spy_helpers import get_count, getAllReturnVals
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.helper import sdk_ensure_pool_functional


@pytest.fixture(scope="module")
def tconf(tconf):
    oldMax3PCBatchSize = tconf.Max3PCBatchSize
    oldMax3PCBatchWait = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = 5
    tconf.Max3PCBatchWait = 2
    yield tconf

    tconf.Max3PCBatchSize = oldMax3PCBatchSize
    tconf.Max3PCBatchWait = oldMax3PCBatchWait


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


def test_node_request_propagates(looper, setup, txnPoolNodeSet,
                                 sdk_wallet_client, sdk_pool_handle, tconf):
    """
    One of node lacks sufficient propagates
    """
    faulty_node, recv_client_requests = setup

    old_count_recv_ppg = get_count(faulty_node, faulty_node.processPropagate)
    old_count_recv_req = get_count(faulty_node, faulty_node.processRequest)

    def sum_of_sent_batches():
        return faulty_node.replicas[0].lastPrePrepareSeqNo + \
               faulty_node.replicas[1].lastPrePrepareSeqNo

    old_sum_of_sent_batches = sum_of_sent_batches()
    old_count_request_propagates = sum_of_request_propagates(faulty_node)

    sent_reqs = 1
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              sent_reqs)
    looper.runFor(tconf.PROPAGATE_REQUEST_DELAY)

    assert get_count(
        faulty_node, faulty_node.processPropagate) > old_count_recv_ppg
    if recv_client_requests:
        assert get_count(
            faulty_node, faulty_node.processRequest) > old_count_recv_req
    else:
        assert get_count(
            faulty_node, faulty_node.processRequest) == old_count_recv_req

    # Attempt to request PROPAGATEs was made as many number of times as the
    # number of sent batches in both replicas since both replicas
    # independently request PROPAGATEs
    assert sum_of_request_propagates(faulty_node) - \
           old_count_request_propagates == (sum_of_sent_batches() -
                                            old_sum_of_sent_batches)

    faulty_node.nodeIbStasher.reset_delays_and_process_delayeds()
    sdk_ensure_pool_functional(looper,
                               txnPoolNodeSet,
                               sdk_wallet_client,
                               sdk_pool_handle,
                               num_reqs=4)
