import pytest
from plenum.test.eventually import eventually
from plenum.test.helper import delay

from plenum.common.request_types import Propagate
from plenum.test.propagate.helper import recvdRequest, recvdPropagate, sentPropagate

nodeCount = 4


@pytest.fixture()
def setup(nodeSet):
    A, B, C, D = nodeSet.nodes.values()  # type: TestNode
    delay(Propagate, frm=[B, C, D], to=A, howlong=5)


def testPropagateRecvdAfterRequest(setup, looper, nodeSet, up, sent1):
    A, B, C, D = nodeSet.nodes.values()  # type: TestNode

    def x():
        # A should have received a request from the client
        assert len(recvdRequest(A)) == 1
        # A should not have received a PROPAGATE
        assert len(recvdPropagate(A)) == 0
        # A should have sent a PROPAGATE
        assert len(sentPropagate(A)) == 1

    looper.run(eventually(x, retryWait=.5, timeout=3))

    def y():
        # A should have received 3 PROPAGATEs
        assert len(recvdPropagate(A)) == 3
        # A should have total of 4 PROPAGATEs (3 from other nodes and 1 from
        # itself)
        key = sent1.clientId, sent1.reqId
        assert len(A.requests[key].propagates) == 4
        # A should still have sent only one PROPAGATE
        assert len(sentPropagate(A)) == 1

    looper.run(eventually(y, retryWait=.5, timeout=7))
