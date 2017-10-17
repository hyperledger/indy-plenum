import random

from plenum.test.client.conftest import passThroughReqAcked1

from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, stopNodes, waitForSufficientRepliesForRequests, \
    send_signed_requests
from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes
from plenum.test.node_catchup.helper import waitNodeDataEquality


nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes("AlphaC")
reqAcked1 = passThroughReqAcked1


def test_client_sends_get_request_to_one_node(looper,
                                              client1,
                                              wallet1,
                                              nodeSet):
    """
    Check that read only equest can be sent
    without having connection to all nodes
    """
    client = client1
    wallet = wallet1

    nodes_to_stop = list(nodeSet)[1:]
    stopNodes(nodes_to_stop, looper)

    initial_submit_count = client.spylog.count(client.submitReqs)
    initial_send_count = client.spylog.count(client.send)

    def sign_and_send(op):
        signed = wallet.signOp(op)
        send_signed_requests(client, [signed])

    buy = {'type': 'buy', 'amount': random.randint(10, 100)}
    sign_and_send(buy)
    assert initial_submit_count + 1 == client.spylog.count(client.submitReqs)
    assert initial_send_count == client.spylog.count(client.send)

    get_buy = {'type': 'get_buy'}
    client._read_only_requests.add('get_buy')
    sign_and_send(get_buy)
    assert initial_submit_count + 2 == client.spylog.count(client.submitReqs)
    assert initial_send_count + 1 == client.spylog.count(client.send)
