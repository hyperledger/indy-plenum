import pytest
from orderedset._orderedset import OrderedSet

from plenum.common.constants import POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.common.messages.node_messages import PrePrepare, Ordered
from plenum.server.propagator import Requests
from plenum.server.replica_freshness_checker import FreshnessChecker
from plenum.test.helper import freshness, sdk_random_request_objects

from plenum.test.replica.conftest import replica as r

FRESHNESS_TIMEOUT = 60

LEDGER_IDS = [POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]


@pytest.fixture(scope='function', params=[0])
def view_no(tconf, request):
    return request.param


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


@pytest.fixture(scope='function')
def replica(r):
    r.node.ledger_ids = LEDGER_IDS
    r.stateRootHash = lambda ledger_id, to_str=False: "EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMA" + str(ledger_id + 1)
    r.txnRootHash = lambda ledger_id, to_str=False: "AuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMA" + str(ledger_id + 1)
    r.node.onBatchCreated = lambda *args: None
    r.isMaster = True
    r.node.requests = Requests()
    r._bls_bft_replica.process_order = lambda *args: None

    # re-init FreshnessChecker to use fake timestamps
    set_current_time(r, 0)
    r._freshness_checker = FreshnessChecker(ledger_ids=r.ledger_ids,
                                            freshness_timeout=r.config.STATE_FRESHNESS_WINDOW,
                                            initial_time=r.get_current_time())
    r.lastBatchCreated = 0
    r.last_accepted_pre_prepare_time = 0

    return r


@pytest.fixture(scope='function')
def replica_with_requests(replica):
    requests = {ledger_id: sdk_random_request_objects(1, identifier="did",
                                                      protocol_version=CURRENT_PROTOCOL_VERSION)[0]
                for ledger_id in LEDGER_IDS}

    def patched_consume_req_queue_for_pre_prepare(ledger_id, tm, view_no, pp_seq_no):
        return [[requests[ledger_id]], [], []]

    replica.consume_req_queue_for_pre_prepare = patched_consume_req_queue_for_pre_prepare

    return replica, requests


def set_current_time(replica, ts):
    replica.get_current_time = lambda: ts


def check_and_pop_ordered(replica, ledger_ids):
    for ledger_id in ledger_ids:
        msg = replica.outBox.popleft()
        assert isinstance(msg, PrePrepare)
        assert msg.ledgerId == ledger_id
        assert len(msg.reqIdr) > 0

    for ledger_id in ledger_ids:
        msg = replica.outBox.popleft()
        assert isinstance(msg, Ordered)
        assert msg.ledgerId == ledger_id

    for ledger_id in ledger_ids:
        replica.requestQueues[ledger_id].clear()


def check_and_pop_freshness_pre_prepare(replica, ledger_id):
    msg = replica.outBox.popleft()
    assert isinstance(msg, PrePrepare)
    assert msg.ledgerId == ledger_id
    assert msg.reqIdr == []


def test_no_freshness_pre_prepare_when_disabled(tconf, replica):
    with freshness(tconf, enabled=False, timeout=FRESHNESS_TIMEOUT):
        assert len(replica.outBox) == 0

        replica.send_3pc_batch()
        assert len(replica.outBox) == 0

        set_current_time(replica, FRESHNESS_TIMEOUT + 1)
        replica.send_3pc_batch()
        assert len(replica.outBox) == 0


def test_freshness_pre_prepare_initially(replica):
    assert len(replica.outBox) == 0
    replica.send_3pc_batch()
    assert len(replica.outBox) == 0


@pytest.mark.parametrize('ts', [
    0, 1, FRESHNESS_TIMEOUT, -1, -FRESHNESS_TIMEOUT
])
def test_freshness_pre_prepare_before_timeout(replica, ts):
    assert len(replica.outBox) == 0
    set_current_time(replica, ts)
    replica.send_3pc_batch()
    assert len(replica.outBox) == 0


def test_freshness_pre_prepare_after_timepout(replica):
    assert len(replica.outBox) == 0
    replica.send_3pc_batch()
    set_current_time(replica, FRESHNESS_TIMEOUT + 1)
    replica.send_3pc_batch()
    assert len(replica.outBox) == 3

    check_and_pop_freshness_pre_prepare(replica, POOL_LEDGER_ID)
    check_and_pop_freshness_pre_prepare(replica, DOMAIN_LEDGER_ID)
    check_and_pop_freshness_pre_prepare(replica, CONFIG_LEDGER_ID)


def test_freshness_pre_prepare_not_resend_before_next_timeout(replica):
    assert len(replica.outBox) == 0

    set_current_time(replica, FRESHNESS_TIMEOUT + 1)
    replica.send_3pc_batch()
    assert len(replica.outBox) == 3

    replica.send_3pc_batch()
    assert len(replica.outBox) == 3

    set_current_time(replica, FRESHNESS_TIMEOUT + 1 + FRESHNESS_TIMEOUT)
    replica.send_3pc_batch()
    assert len(replica.outBox) == 3

    set_current_time(replica, FRESHNESS_TIMEOUT + 1 + FRESHNESS_TIMEOUT + 1)
    replica.send_3pc_batch()
    assert len(replica.outBox) == 6


@pytest.mark.parametrize('ordered, refreshed', [
    ([POOL_LEDGER_ID], [DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID]),
    ([DOMAIN_LEDGER_ID], [POOL_LEDGER_ID, CONFIG_LEDGER_ID]),
    ([CONFIG_LEDGER_ID], [POOL_LEDGER_ID, DOMAIN_LEDGER_ID]),
    ([POOL_LEDGER_ID, DOMAIN_LEDGER_ID], [CONFIG_LEDGER_ID]),
    ([POOL_LEDGER_ID, CONFIG_LEDGER_ID], [DOMAIN_LEDGER_ID]),
    ([DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID], [POOL_LEDGER_ID]),
    ([POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID], [])
])
def test_freshness_pre_prepare_only_when_no_requests_for_ledger(tconf,
                                                                replica_with_requests,
                                                                ordered, refreshed):
    replica, requests = replica_with_requests
    for ordered_ledger_id in ordered:
        replica.requestQueues[ordered_ledger_id] = OrderedSet([requests[ordered_ledger_id].key])

    # send 3PC batch for requests
    assert len(replica.outBox) == 0
    set_current_time(replica, tconf.Max3PCBatchWait + 1)
    replica.send_3pc_batch()
    assert len(replica.outBox) == len(ordered)

    # wait for freshness timeout
    set_current_time(replica, FRESHNESS_TIMEOUT + 1)

    # order requests
    for i in range(len(ordered)):
        replica.order_3pc_key((0, i + 1))
    assert len(replica.outBox) == 2 * len(ordered)
    check_and_pop_ordered(replica, ordered)

    # refresh state for unordered
    replica.send_3pc_batch()
    assert len(replica.outBox) == len(refreshed)
    for refreshed_ledger_id in refreshed:
        check_and_pop_freshness_pre_prepare(replica, refreshed_ledger_id)
