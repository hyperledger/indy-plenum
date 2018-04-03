import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Propagate
from plenum.test.delayers import delay
from plenum.test.helper import assertLength
from plenum.test.propagate.helper import recvdRequest, recvdPropagate, \
    sentPropagate, forwardedRequest
from plenum.test import waits

nodeCount = 4
howlong = 10
delaySec = 5


@pytest.fixture()
def setup(txnPoolNodeSet):
    A, B, C, D = txnPoolNodeSet
    A.clientIbStasher.delay(lambda x: delaySec)
    delay(Propagate, frm=[C, D], to=A, howlong=howlong)


def testPropagateRecvdBeforeRequest(setup, looper, txnPoolNodeSet, sent1):
    A, B, C, D = txnPoolNodeSet

    def x():
        # A should not have received a request from the client
        assert len(recvdRequest(A)) == 0
        # A should have received only one PROPAGATE
        assert len(recvdPropagate(A)) == 1
        # A should have sent only one PROPAGATE
        assert len(sentPropagate(A)) == 1

    timeout = waits.expectedNodeToNodeMessageDeliveryTime() + delaySec - 2
    looper.run(eventually(x, retryWait=.5, timeout=timeout))

    def y():
        # A should have received a request from the client
        assert len(recvdRequest(A)) == 1
        # A should still have sent only one PROPAGATE
        assert len(sentPropagate(A)) == 1

    timeout = waits.expectedNodeToNodeMessageDeliveryTime() + delaySec + 2
    looper.run(eventually(y, retryWait=.5, timeout=timeout))

    def chk():
        # A should have forwarded the request
        assertLength(forwardedRequest(A), 1)

    timeout = waits.expectedClientRequestPropagationTime(
        len(txnPoolNodeSet)) + delaySec
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
