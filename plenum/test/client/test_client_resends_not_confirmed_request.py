import random

from plenum.common.constants import REPLY, OP_FIELD_NAME
from plenum.common.types import f
from plenum.test.spy_helpers import getAllArgs
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.client.conftest import passThroughReqAcked1
from plenum.test.helper import stopNodes, send_signed_requests, \
    waitForSufficientRepliesForRequests
from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes

logger = getlogger()

nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes("AlphaC")
reqAcked1 = passThroughReqAcked1


def fake_send(client):
    old_sendToNodes = client.sendToNodes

    def sendToNodes(msg, names):
        client.sendToNodes = old_sendToNodes
        assert len(names) == 1
        name = names[0]
        old_got_expected = client._got_expected

        def _got_expected(msg, sender):
            if sender == name and msg[OP_FIELD_NAME] == REPLY:
                client._got_expected = old_got_expected
                msg[f.RESULT.nm].pop("state_proof", None)
            return old_got_expected(msg, sender)

        client._got_expected = _got_expected
        return old_sendToNodes(msg, names)

    client.sendToNodes = sendToNodes


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

    fake_send(client)
    buy = {'type': 'get_buy'}
    client._read_only_requests.add('get_buy')
    requests = sign_and_send(buy)
    waitForSufficientRepliesForRequests(looper, client, requests=requests)

    # submitReqs should be called twice: first for but and then got get_buy
    assert initial_submit_count + 2 == \
           client.spylog.count(client.submitReqs)

    assert initial_resent_count + 1 == \
           client.spylog.count(client.resendRequests)
