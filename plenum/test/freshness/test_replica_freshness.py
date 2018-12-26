import pytest

from plenum.common.constants import POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import PrePrepare
from plenum.test.helper import freshness

from plenum.test.replica.conftest import replica as r

FRESHNESS_TIMEOUT = 2


@pytest.fixture(scope='function', params=[0])
def view_no(tconf, request):
    return request.param


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


@pytest.fixture(scope='function')
def replica(r):
    r.isMaster = True
    for ledger_id in r.ledger_ids:
        r._freshness_checker.update_freshness(ledger_id, 0)
    return r


def set_current_time(replica, ts):
    replica.get_current_time = lambda: ts


def check_freshness_pre_prepare(msg, ledger_id):
    assert isinstance(msg, PrePrepare)
    assert msg.ledgerId == ledger_id
    assert msg.reqIdr == []


def test_no_freshness_pre_prepare_when_disabled(tconf, replica):
    with freshness(tconf, enabled=False, timeout=FRESHNESS_TIMEOUT):
        assert len(replica.outBox) == 0
        set_current_time(replica, FRESHNESS_TIMEOUT + 1)
        replica.send_3pc_batch()
        assert len(replica.outBox) == 0


def test_freshness_pre_prepare_for_all_ledgers(replica):
    assert len(replica.outBox) == 0

    set_current_time(replica, FRESHNESS_TIMEOUT + 1)
    replica.send_3pc_batch()
    assert len(replica.outBox) == 3
    check_freshness_pre_prepare(replica.outBox.popleft(), POOL_LEDGER_ID)
    check_freshness_pre_prepare(replica.outBox.popleft(), CONFIG_LEDGER_ID)
    check_freshness_pre_prepare(replica.outBox.popleft(), DOMAIN_LEDGER_ID)


def test_freshness_pre_prepare_ordering_for_one_ledger_only():
    pass


def test_no_freshness_pre_prepare_when_requests_available():
    pass
