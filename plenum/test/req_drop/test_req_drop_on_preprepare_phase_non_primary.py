import pytest

from plenum.common.constants import PROPAGATE
from plenum.test.helper import sdk_send_random_requests
from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.test.delayers import delay, msg_rep_delay
from plenum.test.propagate.helper import recvdRequest, recvdPropagate, \
    sentPropagate, recvdPrePrepareForInstId, recvdPrepareForInstId, recvdCommitForInstId
from plenum.test.test_node import TestNode
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.spy_helpers import getAllArgs

howlong = 20
initial_ledger_size = 0


@pytest.fixture(scope="module")
def tconf(tconf):
    OUTDATED_REQS_CHECK_ENABLED_OLD = tconf.OUTDATED_REQS_CHECK_ENABLED
    OUTDATED_REQS_CHECK_INTERVAL_OLD = tconf.OUTDATED_REQS_CHECK_INTERVAL
    PROPAGATES_PHASE_REQ_TIMEOUT_OLD = tconf.PROPAGATES_PHASE_REQ_TIMEOUT
    ORDERING_PHASE_REQ_TIMEOUT_OLD = tconf.ORDERING_PHASE_REQ_TIMEOUT
    PROPAGATE_REQUEST_DELAY_OLD = tconf.PROPAGATE_REQUEST_DELAY

    tconf.OUTDATED_REQS_CHECK_ENABLED = True
    tconf.OUTDATED_REQS_CHECK_INTERVAL = 1
    tconf.PROPAGATES_PHASE_REQ_TIMEOUT = 3600
    tconf.ORDERING_PHASE_REQ_TIMEOUT = 10
    tconf.PROPAGATE_REQUEST_DELAY = 0
    yield tconf

    tconf.OUTDATED_REQS_CHECK_ENABLED = OUTDATED_REQS_CHECK_ENABLED_OLD
    tconf.OUTDATED_REQS_CHECK_INTERVAL = OUTDATED_REQS_CHECK_INTERVAL_OLD
    tconf.PROPAGATES_PHASE_REQ_TIMEOUT = PROPAGATES_PHASE_REQ_TIMEOUT_OLD
    tconf.ORDERING_PHASE_REQ_TIMEOUT = ORDERING_PHASE_REQ_TIMEOUT_OLD
    tconf.PROPAGATE_REQUEST_DELAY = PROPAGATE_REQUEST_DELAY_OLD


@pytest.fixture()
def setup(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    global initial_ledger_size
    A, B, C, D = txnPoolNodeSet  # type: TestNode
    lagged_node = C
    frm = [A, B, D]
    delay(PrePrepare, frm=frm, to=lagged_node, howlong=howlong)
    delay(Prepare, frm=frm, to=lagged_node, howlong=howlong + 2)
    delay(Commit, frm=frm, to=lagged_node, howlong=howlong + 4)
    # Delay MessageRep by long simulating loss as if Propagate is missing, it
    # is requested
    A.nodeIbStasher.delay(msg_rep_delay(10 * howlong, [PROPAGATE, ]))
    initial_ledger_size = lagged_node.domainLedger.size
    request_couple_json = sdk_send_random_requests(
        looper, sdk_pool_handle, sdk_wallet_client, 1)
    return request_couple_json


def test_req_drop_on_preprepare_phase_on_non_primary_and_then_ordered(
        tconf, setup, looper, txnPoolNodeSet,
        sdk_wallet_client, sdk_pool_handle):
    global initial_ledger_size
    A, B, C, D = txnPoolNodeSet  # type: TestNode
    lagged_node = C

    def check_preprepares_delayed():
        # Node should have received a request from the client
        assert len(recvdRequest(lagged_node)) == 1
        # Node should not have received a PROPAGATE
        assert len(recvdPropagate(lagged_node)) == 3
        # Node should have sent a PROPAGATE
        assert len(sentPropagate(lagged_node)) == 1
        # Node should have not received PrePrepares for master instance
        assert len(recvdPrePrepareForInstId(lagged_node, 0)) == 0
        # Node should have not received Prepares for master instance
        assert len(recvdPrepareForInstId(lagged_node, 0)) == 0
        # Node should have not received Commits for master instance
        assert len(recvdCommitForInstId(lagged_node, 0)) == 0
        # Node should have 1 request in requests queue
        assert len(lagged_node.requests) == 1

    timeout = howlong - 2
    looper.run(eventually(check_preprepares_delayed, retryWait=.5, timeout=timeout))

    def check_drop():
        # Node should have not received PrePrepare, Prepares and Commits for master instance
        assert len(recvdPrePrepareForInstId(lagged_node, 0)) == 0
        assert len(recvdPrepareForInstId(lagged_node, 0)) == 0
        assert len(recvdCommitForInstId(lagged_node, 0)) == 0
        # Request object should be dropped by timeout
        assert len(lagged_node.requests) == 0

    timeout = tconf.ORDERING_PHASE_REQ_TIMEOUT + tconf.OUTDATED_REQS_CHECK_INTERVAL + 1
    looper.run(eventually(check_drop, retryWait=.5, timeout=timeout))

    for n in txnPoolNodeSet:
        n.nodeIbStasher.resetDelays()

    def check_propagates_requested():
        # Node should have received delayed PrePrepare
        assert len(recvdPrePrepareForInstId(lagged_node, 0)) >= 1
        # Check that PROPAGATEs are requested by at least one replica as PrePrepare has been
        # received for request that was dropped.
        # We can check that the number of requested propagates is more or equal to 1,
        # since the first replica who sees non-finalized requests sends MessageReq for Propagates,
        # and it can receive Propagates before a PrePrepare for the next replica is received,
        # so that the for the second replica all requests will be already finalized.
        assert len(getAllArgs(lagged_node, TestNode.request_propagates)) == 2

    timeout = howlong
    looper.run(eventually(check_propagates_requested, retryWait=.5, timeout=timeout))

    def check_propagates_and_3pc_received():
        # Node should not have received requested PROPAGATEs
        assert len(recvdPropagate(lagged_node)) == 6
        # Node should have received delayed Prepares and Commits for master instance
        assert len(recvdPrepareForInstId(lagged_node, 0)) == 2
        assert len(recvdCommitForInstId(lagged_node, 0)) == 3

    timeout = howlong + 2
    looper.run(eventually(check_propagates_and_3pc_received, retryWait=.5, timeout=timeout))

    def check_ledger_size():
        # The request should be eventually ordered
        for n in txnPoolNodeSet:
            assert n.domainLedger.size - initial_ledger_size == 1

    looper.run(eventually(check_ledger_size, retryWait=.5, timeout=timeout))

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
