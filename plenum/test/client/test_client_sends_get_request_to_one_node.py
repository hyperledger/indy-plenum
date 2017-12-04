import random

from plenum.test.spy_helpers import getAllArgs
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.client.conftest import passThroughReqAcked1
from plenum.test.helper import stopNodes, send_signed_requests

from plenum.test.helper import stopNodes, send_signed_requests
from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes

logger = getlogger()

nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes("AlphaC")
reqAcked1 = passThroughReqAcked1


def test_client_sends_get_request_to_one_node(looper,
                                              client1,
                                              wallet1,
                                              nodeSet):
    """
    Check that client sends read only request to one node only
    """
    client = client1
    wallet = wallet1

    def sign_and_send(op):
        signed = wallet.signOp(op)
        send_signed_requests(client, [signed])

    logger.info("Send set request")
    buy = {'type': 'buy', 'amount': random.randint(10, 100)}
    sign_and_send(buy)

    send_args = getAllArgs(client, client.send)
    rids = send_args[0]['rids']
    assert len(rids) > 1

    logger.info("Send get request")
    get_buy = {'type': 'get_buy'}
    client._read_only_requests.add('get_buy')
    sign_and_send(get_buy)

    send_args = getAllArgs(client, client.send)
    rids = send_args[0]['rids']
    assert len(rids) == 1


def test_client_can_send_get_request_to_one_node(looper,
                                                 client1,
                                                 wallet1,
                                                 nodeSet):
    """
    Check that read only request can be sent
    without having connection to all nodes
    """
    client = client1
    wallet = wallet1

    logger.info("Stopping nodes")
    nodes_to_stop = list(nodeSet)[1:]
    stopNodes(nodes_to_stop, looper)

    def sign_and_send(op):
        signed = wallet.signOp(op)
        send_signed_requests(client, [signed])

    def check_client_disconnected():
        assert not client.hasSufficientConnections

    logger.info("Ensure that client has disconnected from the stopped nodes")
    # TODO non-default timeout
    looper.run(eventually(check_client_disconnected, retryWait=1))

    initial_submit_count = client.spylog.count(client.submitReqs)
    initial_send_count = client.spylog.count(client.send)

    logger.info("Send set request")
    buy = {'type': 'buy', 'amount': random.randint(10, 100)}
    sign_and_send(buy)
    assert initial_submit_count + 1 == client.spylog.count(client.submitReqs)
    assert initial_send_count == client.spylog.count(client.send)

    logger.info("Send get request")
    get_buy = {'type': 'get_buy'}
    client._read_only_requests.add('get_buy')
    sign_and_send(get_buy)
    assert initial_submit_count + 2 == client.spylog.count(client.submitReqs)
    assert initial_send_count + 1 == client.spylog.count(client.send)
