import types
import pytest

from plenum.common.constants import PROPAGATE
from plenum.test.helper import sdk_json_to_request_object, sdk_send_random_requests
from plenum.test.spy_helpers import get_count
from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Propagate
from plenum.test.delayers import delay, msg_rep_delay
from plenum.test.propagate.helper import recvdRequest, recvdPropagate, \
    sentPropagate
from plenum.test.test_node import TestNode

nodeCount = 4
howlong = 5
reqCount = 1


@pytest.fixture()
def setup(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client,):
    def _clean(*args):
        pass

    A, B, C, D = txnPoolNodeSet  # type: TestNode
    delay(Propagate, frm=[B, C, D], to=A, howlong=howlong)
    # Delay MessageRep by long simulating loss as if Propagate is missing, it
    # is requested
    A.nodeIbStasher.delay(msg_rep_delay(10 * howlong, [PROPAGATE, ]))
    # disable _clean method which remove req.key from requests map
    A.requests._clean = types.MethodType(
                            _clean, A.requests)
    request_couple_json = sdk_send_random_requests(
        looper, sdk_pool_handle, sdk_wallet_client, reqCount)
    return request_couple_json


def testPropagateRecvdAfterRequest(setup, looper, txnPoolNodeSet):
    A, B, C, D = txnPoolNodeSet  # type: TestNode
    sent1 = sdk_json_to_request_object(setup[0][0])

    def x():
        # A should have received a request from the client
        assert len(recvdRequest(A)) == 1
        # A should not have received a PROPAGATE
        assert len(recvdPropagate(A)) == 0
        # A should have sent a PROPAGATE
        assert len(sentPropagate(A)) == 1

    timeout = howlong - 2
    looper.run(eventually(x, retryWait=.5, timeout=timeout))
    for n in txnPoolNodeSet:
        n.nodeIbStasher.resetDelays()

    def y():
        # A should have received 3 PROPAGATEs
        assert len(recvdPropagate(A)) == 3
        # A should have total of 4 PROPAGATEs (3 from other nodes and 1 from
        # itself)
        key = sent1.digest
        assert key in A.requests
        assert len(A.requests[key].propagates) == 4
        # A should still have sent only one PROPAGATE
        assert len(sentPropagate(A)) == 1

    timeout = howlong + 2
    looper.run(eventually(y, retryWait=.5, timeout=timeout))
    auth_obj = A.authNr(0).core_authenticator
    auth_calling_count = get_count(auth_obj, auth_obj.authenticate)
    assert auth_calling_count == reqCount
