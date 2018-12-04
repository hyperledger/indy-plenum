import pytest

from plenum.common.constants import PROPAGATE
from plenum.test.helper import sdk_json_to_request_object, sdk_send_random_requests
from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Propagate
from plenum.test.delayers import delay, msg_rep_delay
from plenum.test.propagate.helper import recvdRequest, recvdPropagate, \
    sentPropagate
from plenum.test.test_node import TestNode
from plenum.test.node_request.helper import sdk_ensure_pool_functional


howlong = 20
initial_ledger_size = 0


@pytest.fixture(scope="module")
def tconf(tconf):
    OUTDATED_REQS_CHECK_ENABLED_OLD = tconf.OUTDATED_REQS_CHECK_ENABLED
    OUTDATED_REQS_CHECK_INTERVAL_OLD = tconf.OUTDATED_REQS_CHECK_INTERVAL
    PROPAGATES_PHASE_REQ_TIMEOUT_OLD = tconf.PROPAGATES_PHASE_REQ_TIMEOUT
    ORDERING_PHASE_REQ_TIMEOUT_OLD = tconf.ORDERING_PHASE_REQ_TIMEOUT

    tconf.OUTDATED_REQS_CHECK_ENABLED = True
    tconf.OUTDATED_REQS_CHECK_INTERVAL = 1
    tconf.PROPAGATES_PHASE_REQ_TIMEOUT = 3
    tconf.ORDERING_PHASE_REQ_TIMEOUT = 3600
    yield tconf

    tconf.OUTDATED_REQS_CHECK_ENABLED = OUTDATED_REQS_CHECK_ENABLED_OLD
    tconf.OUTDATED_REQS_CHECK_INTERVAL = OUTDATED_REQS_CHECK_INTERVAL_OLD
    tconf.PROPAGATES_PHASE_REQ_TIMEOUT = PROPAGATES_PHASE_REQ_TIMEOUT_OLD
    tconf.ORDERING_PHASE_REQ_TIMEOUT = ORDERING_PHASE_REQ_TIMEOUT_OLD


@pytest.fixture()
def setup(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    global initial_ledger_size
    A, B, C, D = txnPoolNodeSet  # type: TestNode
    delay(Propagate, frm=[B, C, D], to=A, howlong=howlong)
    # Delay MessageRep by long simulating loss as if Propagate is missing, it
    # is requested
    A.nodeIbStasher.delay(msg_rep_delay(10 * howlong, [PROPAGATE, ]))
    initial_ledger_size = txnPoolNodeSet[0].domainLedger.size
    request_couple_json = sdk_send_random_requests(
        looper, sdk_pool_handle, sdk_wallet_client, 1)
    return request_couple_json


def test_req_drop_on_propagate_phase_on_master_primary_and_then_ordered(
        tconf, setup, looper, txnPoolNodeSet,
        sdk_wallet_client, sdk_pool_handle):
    global initial_ledger_size
    A, B, C, D = txnPoolNodeSet  # type: TestNode
    sent1 = sdk_json_to_request_object(setup[0][0])

    def check_propagates_delayed():
        # A should have received a request from the client
        assert len(recvdRequest(A)) == 1
        # A should not have received a PROPAGATE
        assert len(recvdPropagate(A)) == 0
        # A should have sent a PROPAGATE
        assert len(sentPropagate(A)) == 1
        assert len(A.requests) == 1

    timeout = howlong - 2
    looper.run(eventually(check_propagates_delayed, retryWait=.5, timeout=timeout))

    def check_drop():
        assert len(A.requests) == 0

    timeout = tconf.PROPAGATES_PHASE_REQ_TIMEOUT + tconf.OUTDATED_REQS_CHECK_INTERVAL + 1
    looper.run(eventually(check_drop, retryWait=.5, timeout=timeout))

    for n in txnPoolNodeSet:
        n.nodeIbStasher.resetDelays()

    def check_propagates_received():
        # A should have received 3 PROPAGATEs
        assert len(recvdPropagate(A)) == 3
        # A should have total of 4 PROPAGATEs (3 from other nodes and 1 from
        # itself)
        key = sent1.digest
        assert key in A.requests
        assert len(A.requests[key].propagates) == 4
        # A should still have sent two PROPAGATEs since request
        # was dropped and re-received over propagate
        assert len(sentPropagate(A)) == 2

    timeout = howlong + 2
    looper.run(eventually(check_propagates_received, retryWait=.5, timeout=timeout))

    def check_ledger_size():
        # The request should be eventually ordered
        for node in txnPoolNodeSet:
            assert node.domainLedger.size - initial_ledger_size == 1

    looper.run(eventually(check_ledger_size, retryWait=.5, timeout=timeout))

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
