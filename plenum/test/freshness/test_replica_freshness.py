from plenum.common.constants import CONFIG_LEDGER_ID
from plenum.common.messages.node_messages import Ordered
from plenum.test.helper import freshness, assertExp

from plenum.test.replica.conftest import *
from plenum.test.test_node import getPrimaryReplica
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 60
OLDEST_TS = 1499906903

LEDGER_IDS = [POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]


@pytest.fixture(scope='function', params=[0])
def viewNo(tconf, request):
    return request.param


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


@pytest.fixture(scope='function')
def mock_timestamp():
    return MockTimestamp(OLDEST_TS)


@pytest.fixture(scope='function')
def ledger_ids():
    return LEDGER_IDS


@pytest.fixture(scope='function', params=[0])
def inst_id(request):
    return request.param


@pytest.fixture(scope='function')
def replica_with_valid_requests(replica):
    requests = {ledger_id: sdk_random_request_objects(1, identifier="did",
                                                      protocol_version=CURRENT_PROTOCOL_VERSION)[0]
                for ledger_id in LEDGER_IDS}

    def patched_consume_req_queue_for_pre_prepare(ledger_id, tm, view_no, pp_seq_no):
        reqs = [requests[ledger_id]] if len(replica.requestQueues[ledger_id]) > 0 else []
        return [reqs, [], []]

    replica.consume_req_queue_for_pre_prepare = patched_consume_req_queue_for_pre_prepare

    return replica, requests


def set_current_time(replica, ts):
    replica.get_current_time.value = OLDEST_TS + ts
    replica.get_time_for_3pc_batch.value = OLDEST_TS + ts


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


def test_no_freshness_pre_prepare_for_non_master(tconf, replica):
    replica.isMaster = False
    replica.instId = 1
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
                                                                replica_with_valid_requests,
                                                                ordered, refreshed):
    replica, requests = replica_with_valid_requests
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


def test_order_empty_pre_prepare(looper, tconf, txnPoolNodeSet):
    assert all(node.master_replica.last_ordered_3pc == (0, 0) for node in txnPoolNodeSet)
    assert all(node.spylog.count(node.processOrdered) == 0 for node in txnPoolNodeSet)

    replica = getPrimaryReplica([txnPoolNodeSet[0]], instId=0)
    replica._do_send_3pc_batch(ledger_id=POOL_LEDGER_ID)

    looper.run(eventually(
        lambda: assertExp(
            all(
                node.master_replica.last_ordered_3pc == (0, 1) for node in txnPoolNodeSet
            )
        )
    ))
    looper.run(eventually(
        lambda: assertExp(
            all(
                node.spylog.count(node.processOrdered) == 1 for node in txnPoolNodeSet
            )
        )
    ))
