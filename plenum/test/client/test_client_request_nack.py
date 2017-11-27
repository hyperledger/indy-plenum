from functools import partial

import pytest

from stp_core.loop.eventually import eventuallyAll

from plenum.test import waits
from plenum.test.helper import checkReqNack

whitelist = ['discarding message']


class TestVerifier:
    @staticmethod
    def verify(operation):
        assert operation['amount'] <= 100, 'amount too high'


@pytest.fixture(scope="module")
def restrictiveVerifier(nodeSet):
    for n in nodeSet:
        n.opVerifiers = [TestVerifier()]


@pytest.fixture(scope="module")
def request1(wallet1):
    op = {"type": "buy",
          "amount": 999}
    req = wallet1.signOp(op)
    return req


@pytest.mark.skip(reason="old style plugin")
def testRequestFullRoundTrip(restrictiveVerifier,
                             client1,
                             sent1,
                             looper,
                             nodeSet):

    update = {'reason': 'client request invalid: InvalidClientRequest() '
                        '[caused by amount too high\nassert 999 <= 100]'}

    coros2 = [partial(checkReqNack, client1, node, sent1.identifier,
                      sent1.reqId, update)
              for node in nodeSet]
    timeout = waits.expectedReqAckQuorumTime()
    looper.run(eventuallyAll(*coros2, totalTimeout=timeout))
