import random

from stp_core.common.log import getlogger
from plenum.test.client.conftest import passThroughReqAcked1
from plenum.test.helper import send_signed_requests, \
    waitForSufficientRepliesForRequests
from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes

logger = getlogger()

nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes("AlphaC")
reqAcked1 = passThroughReqAcked1
nodes_with_bls = 0


def test_client_resends_not_confirmed_request(looper,
                                              client1,
                                              wallet1,
                                              nodeSet):
    """
    Check that client resends request to all nodes if it was previously sent
    to one node but reply cannot be verified
    """
    client = client1
    wallet = wallet1

    initial_submit_count = client.spylog.count(client.submitReqs)
    initial_resent_count = client.spylog.count(client.resendRequests)

    def sign_and_send(op):
        signed = wallet.signOp(op)
        return send_signed_requests(client, [signed])

    buy = {'type': 'buy', 'amount': random.randint(10, 100)}
    requests = sign_and_send(buy)
    waitForSufficientRepliesForRequests(looper, client, requests=requests)

    buy = {'type': 'get_buy'}
    client._read_only_requests.add('get_buy')
    requests = sign_and_send(buy)
    waitForSufficientRepliesForRequests(looper, client, requests=requests)

    # submitReqs should be called twice: first for but and then got get_buy
    assert initial_submit_count + 2 == \
           client.spylog.count(client.submitReqs)

    assert initial_resent_count + 1 == \
           client.spylog.count(client.resendRequests)
