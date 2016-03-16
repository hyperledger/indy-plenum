from functools import partial

import pytest

from plenum.common.txn import REQNACK
from plenum.test.eventually import eventuallyAll
from plenum.test.helper import checkReqAck

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
def request1():
    return {"type": "buy", "amount": 999}


def testRequestFullRoundTrip(restrictiveVerifier,
                             client1,
                             sent1,
                             looper,
                             nodeSet):

    update = {'op': 'REQNACK',
              'reason': 'client request invalid: AssertionError amount too '
                        'high\nassert 999 <= 100'}

    coros2 = [partial(checkReqAck, client1, node, sent1.reqId, update)
              for node in nodeSet]
    looper.run(eventuallyAll(*coros2, totalTimeout=5))
