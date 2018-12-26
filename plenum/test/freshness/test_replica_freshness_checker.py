import pytest

from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.server.replica_freshness import FreshnessChecker

FRESHNESS_TIMEOUT = 60


@pytest.fixture()
def freshness_checker():
    return FreshnessChecker(initial_time=0,
                            freshness_timeout=FRESHNESS_TIMEOUT,
                            ledger_ids=[POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID])


def test_check_freshness(freshness_checker):
    freshness_checker.check_frehsness(0)
    assert len(freshness_checker.get_outdated_ledgers()) == 0

    freshness_checker.check_frehsness(1)
    assert len(freshness_checker.get_outdated_ledgers()) == 0

    freshness_checker.check_frehsness(FRESHNESS_TIMEOUT)
    assert len(freshness_checker.get_outdated_ledgers()) == 0

    freshness_checker.check_frehsness(-1)
    assert len(freshness_checker.get_outdated_ledgers()) == 0

    freshness_checker.check_frehsness(-FRESHNESS_TIMEOUT)
    assert len(freshness_checker.get_outdated_ledgers()) == 0

    freshness_checker.check_frehsness(FRESHNESS_TIMEOUT + 1)
    assert len(freshness_checker.get_outdated_ledgers()) == 3


def test_get_outdated_ledgers(freshness_checker):
    freshness_checker.check_frehsness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)]


def test_update_freshness_one_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    freshness_checker.check_frehsness(FRESHNESS_TIMEOUT)
    assert freshness_checker.get_outdated_ledgers() == []

    freshness_checker.check_frehsness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)]

    freshness_checker.check_frehsness(10 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 11),
                                                        (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 11)]


def test_update_freshness_multiple_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=30)
    freshness_checker.check_frehsness(FRESHNESS_TIMEOUT)
    assert freshness_checker.get_outdated_ledgers() == []

    freshness_checker.check_frehsness(10 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1)]

    freshness_checker.check_frehsness(20 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 11)]

    freshness_checker.check_frehsness(30 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 11),
                                                        (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 21)]


def test_get_outdated_ledgers_sorted(freshness_checker):
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=30)

    freshness_checker.check_frehsness(30 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 11),
                                                        (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 21)]

    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30 + 10)
    freshness_checker.check_frehsness(30 + FRESHNESS_TIMEOUT + 15)
    assert freshness_checker.get_outdated_ledgers() == [(POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 5),
                                                        (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 15),
                                                        (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 25)]

    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=30 + 15)
    freshness_checker.check_frehsness(30 + FRESHNESS_TIMEOUT + 30)
    assert freshness_checker.get_outdated_ledgers() == [(DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 15),
                                                        (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 20),
                                                        (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 30)]

    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=30 + 13)
    freshness_checker.check_frehsness(30 + FRESHNESS_TIMEOUT + 40)
    assert freshness_checker.get_outdated_ledgers() == [(DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 25),
                                                        (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 27),
                                                        (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 30)]


def test_pop_next_outdated_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=30)
    freshness_checker.check_frehsness(30 + FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == [(CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 11),
                                                        (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 21)]

    assert freshness_checker.pop_next_outdated_ledger() == (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 21)
    assert freshness_checker.get_outdated_ledgers() == [(CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
                                                        (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 11)]

    assert freshness_checker.pop_next_outdated_ledger() == (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 11)
    assert freshness_checker.get_outdated_ledgers() == [(CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)]

    assert freshness_checker.pop_next_outdated_ledger() == (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.get_outdated_ledgers() == []

    assert freshness_checker.pop_next_outdated_ledger() == (None, None)
    assert freshness_checker.get_outdated_ledgers() == []
