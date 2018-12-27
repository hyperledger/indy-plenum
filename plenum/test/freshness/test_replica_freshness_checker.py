import pytest

from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.server.replica_freshness_checker import FreshnessChecker

FRESHNESS_TIMEOUT = 60

LEDGER_IDS = [POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]
LEDGER_COUNT = len(LEDGER_IDS)


@pytest.fixture()
def freshness_checker():
    return FreshnessChecker(initial_time=0,
                            freshness_timeout=FRESHNESS_TIMEOUT,
                            ledger_ids=[POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID])


@pytest.mark.parametrize('ts', [
    0, 1, FRESHNESS_TIMEOUT, -1, -FRESHNESS_TIMEOUT
])
def test_check_freshness_before_timeout(freshness_checker, ts):
    freshness_checker.check_freshness(ts)
    assert freshness_checker.get_outdated_ledgers_count() == 0


def test_check_freshness_after_timeout(freshness_checker):
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == LEDGER_COUNT


def test_check_freshness_call_multiple_times_after_timeout(freshness_checker):
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == LEDGER_COUNT

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == LEDGER_COUNT

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 2)
    assert freshness_checker.get_outdated_ledgers_count() == LEDGER_COUNT


def test_pop_outdated_ledger(freshness_checker):
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == LEDGER_COUNT
    for i in range(LEDGER_COUNT):
        assert freshness_checker.pop_next_outdated_ledger() is not None
    assert freshness_checker.get_outdated_ledgers_count() == 0
    assert freshness_checker.pop_next_outdated_ledger() is None


@pytest.mark.parametrize('ts', [
    FRESHNESS_TIMEOUT + 1,
    FRESHNESS_TIMEOUT + 2,
    FRESHNESS_TIMEOUT + FRESHNESS_TIMEOUT,
    FRESHNESS_TIMEOUT + FRESHNESS_TIMEOUT + 1,
])
def test_outdated_ledgers_are_not_checked_until_timeout(freshness_checker, ts):
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == LEDGER_COUNT
    for i in range(LEDGER_COUNT):
        assert freshness_checker.pop_next_outdated_ledger() is not None

    freshness_checker.check_freshness(ts)
    assert freshness_checker.get_outdated_ledgers_count() == 0
    assert freshness_checker.pop_next_outdated_ledger() is None

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == LEDGER_COUNT


def test_pop_outdated_ledgers_same_ts_sorted_by_ledger(freshness_checker):
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)


def test_pop_outdated_ledgers_sorted_by_time(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30)

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.get_outdated_ledgers_count() == 3
    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 10)
    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 20)
    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 30)


def test_check_freshness_updates_ts(freshness_checker):
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.get_outdated_ledgers_count() == 3
    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000)


def test_update_freshness_one_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT)
    assert freshness_checker.get_outdated_ledgers_count() == 0

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == 2
    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)

    freshness_checker.check_freshness(10 + FRESHNESS_TIMEOUT)
    assert freshness_checker.get_outdated_ledgers_count() == 0

    freshness_checker.check_freshness(10 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == 1
    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1)

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == 0

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.get_outdated_ledgers_count() == 3
    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 10)


def test_update_freshness_multiple_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30)
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT)
    assert freshness_checker.get_outdated_ledgers_count() == 0

    freshness_checker.check_freshness(10 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == 1
    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1)

    freshness_checker.check_freshness(20 + FRESHNESS_TIMEOUT)
    assert freshness_checker.get_outdated_ledgers_count() == 0

    freshness_checker.check_freshness(20 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == 1
    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)

    freshness_checker.check_freshness(30 + FRESHNESS_TIMEOUT)
    assert freshness_checker.get_outdated_ledgers_count() == 0

    freshness_checker.check_freshness(30 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers_count() == 1
    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1)

    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000)
    assert freshness_checker.get_outdated_ledgers_count() == 3
    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 10)
    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 20)
    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 30)
