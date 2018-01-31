import pytest

from plenum.common.constants import PROPAGATE
from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Propagate
from plenum.test.delayers import delay, msg_rep_delay
from plenum.test.propagate.helper import recvdRequest, recvdPropagate, \
    sentPropagate
from plenum.test.test_node import TestNode

nodeCount = 4
howlong = 5


@pytest.fixture()
def setup(nodeSet):
    A, B, C, D = nodeSet.nodes.values()  # type: TestNode
    delay(Propagate, frm=[B, C, D], to=A, howlong=howlong)
    # Delay MessageRep by long simulating loss as if Propagate is missing, it
    # is requested
    A.nodeIbStasher.delay(msg_rep_delay(10*howlong, [PROPAGATE, ]))


def testPropagateRecvdAfterRequest(setup, looper, nodeSet, up, sent1):
    A, B, C, D = nodeSet.nodes.values()  # type: TestNode

    def x():
        # A should have received a request from the client
        assert len(recvdRequest(A)) == 1
        # A should not have received a PROPAGATE
        assert len(recvdPropagate(A)) == 0
        # A should have sent a PROPAGATE
        assert len(sentPropagate(A)) == 1

    timeout = howlong - 2
    looper.run(eventually(x, retryWait=.5, timeout=timeout))

    def y():
        # A should have received 3 PROPAGATEs
        assert len(recvdPropagate(A)) == 3
        # A should have total of 4 PROPAGATEs (3 from other nodes and 1 from
        # itself)
        key = sent1.identifier, sent1.reqId
        assert len(A.requests[key].propagates) == 4
        # A should still have sent only one PROPAGATE
        assert len(sentPropagate(A)) == 1

    timeout = howlong + 2
    looper.run(eventually(y, retryWait=.5, timeout=timeout))
