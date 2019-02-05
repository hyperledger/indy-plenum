from collections import OrderedDict

import pytest

from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.server.replica_freshness_checker import FreshnessChecker

FRESHNESS_TIMEOUT = 60

LEDGER_IDS = [POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]
LEDGER_COUNT = len(LEDGER_IDS)

NEW_LEDGER_ID = 1000


@pytest.fixture()
def freshness_checker():
    fc = FreshnessChecker(freshness_timeout=FRESHNESS_TIMEOUT)
    for ledger_id in LEDGER_IDS:
        fc.register_ledger(ledger_id=ledger_id,
                           initial_time=0)
    return fc


@pytest.mark.parametrize('ts', [
    0, 1, FRESHNESS_TIMEOUT, -1, -FRESHNESS_TIMEOUT
])
def test_check_freshness_before_timeout(freshness_checker, ts):
    assert len(freshness_checker.check_freshness(ts)) == 0


def test_check_freshness_after_timeout(freshness_checker):
    assert len(freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)) == LEDGER_COUNT


@pytest.mark.parametrize('ts', [
    FRESHNESS_TIMEOUT + 1,
    FRESHNESS_TIMEOUT + 2,
    FRESHNESS_TIMEOUT + FRESHNESS_TIMEOUT,
    FRESHNESS_TIMEOUT + FRESHNESS_TIMEOUT + 1,
])
def test_check_freshness_not_checked_until_timeout(freshness_checker, ts):
    assert len(freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)) == LEDGER_COUNT
    assert len(freshness_checker.check_freshness(ts)) == 0
    assert len(freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1 + FRESHNESS_TIMEOUT + 1)) == LEDGER_COUNT


def test_check_freshness_result_same_time_sorted_by_ledger(freshness_checker):
    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])


def test_check_freshness_result_sorted_by_time(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30)
    a = freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000)
    assert a == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 10),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 20),
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 30)
           ])


def test_check_freshness_updates_ts(freshness_checker):
    freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1)
    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000) == \
           OrderedDict([
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000),
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000),
           ])


def test_update_freshness_one_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    assert len(freshness_checker.check_freshness(FRESHNESS_TIMEOUT)) == 0

    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])

    assert len(freshness_checker.check_freshness(10 + FRESHNESS_TIMEOUT)) == 0

    assert freshness_checker.check_freshness(10 + FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])

    assert len(freshness_checker.check_freshness(FRESHNESS_TIMEOUT + FRESHNESS_TIMEOUT + 1)) == 0

    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000) == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000),
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 10)
           ])


def test_update_freshness_multiple_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30)

    assert len(freshness_checker.check_freshness(FRESHNESS_TIMEOUT)) == 0

    assert freshness_checker.check_freshness(10 + FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])

    assert len(freshness_checker.check_freshness(20 + FRESHNESS_TIMEOUT)) == 0

    assert freshness_checker.check_freshness(20 + FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])

    assert len(freshness_checker.check_freshness(30 + FRESHNESS_TIMEOUT)) == 0

    assert freshness_checker.check_freshness(30 + FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])

    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1000) == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 10),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 20),
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1000 - 30)
           ])


def test_register_new_ledger_zero_initial_time(freshness_checker):
    freshness_checker.register_ledger(ledger_id=NEW_LEDGER_ID, initial_time=0)
    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (NEW_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])


def test_register_new_ledger_non_zero_initial_time(freshness_checker):
    freshness_checker.register_ledger(ledger_id=NEW_LEDGER_ID, initial_time=10)
    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 1) == \
           OrderedDict([
               (POOL_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (DOMAIN_LEDGER_ID, FRESHNESS_TIMEOUT + 1),
               (CONFIG_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])

    assert len(freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 10)) == 0
    assert freshness_checker.check_freshness(FRESHNESS_TIMEOUT + 10 + 1) == \
           OrderedDict([
               (NEW_LEDGER_ID, FRESHNESS_TIMEOUT + 1)
           ])


def test_get_last_update_time_before_check_freshness(freshness_checker):
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (POOL_LEDGER_ID, 0),
               (DOMAIN_LEDGER_ID, 0),
               (CONFIG_LEDGER_ID, 0)
           ])


@pytest.mark.parametrize('ts', [
    0, 1, FRESHNESS_TIMEOUT, -1, -FRESHNESS_TIMEOUT, FRESHNESS_TIMEOUT + 1
])
def test_get_last_update_time_after_check_freshness(freshness_checker, ts):
    freshness_checker.check_freshness(ts)
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (POOL_LEDGER_ID, 0),
               (DOMAIN_LEDGER_ID, 0),
               (CONFIG_LEDGER_ID, 0)
           ])


def test_get_last_update_time_after_update_freshness(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    assert len(freshness_checker.get_last_update_time()) == 3


def test_get_last_update_time_sorted_by_ts(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30)
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, 10),
               (CONFIG_LEDGER_ID, 20),
               (POOL_LEDGER_ID, 30)
           ])


def test_get_last_update_time_same_time_sorted_by_ledger(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=10)
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (POOL_LEDGER_ID, 10),
               (DOMAIN_LEDGER_ID, 10),
               (CONFIG_LEDGER_ID, 10)
           ])


def test_get_last_update_time_after_last_update_freshness(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (POOL_LEDGER_ID, 0),
               (CONFIG_LEDGER_ID, 0),
               (DOMAIN_LEDGER_ID, 10),
           ])

    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=20)
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (POOL_LEDGER_ID, 0),
               (DOMAIN_LEDGER_ID, 10),
               (CONFIG_LEDGER_ID, 20),
           ])

    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30)
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, 10),
               (CONFIG_LEDGER_ID, 20),
               (POOL_LEDGER_ID, 30),
           ])


def test_get_last_update_time_after_update_and_check_freshness(freshness_checker):
    freshness_checker.update_freshness(ledger_id=DOMAIN_LEDGER_ID, ts=10)
    freshness_checker.update_freshness(ledger_id=CONFIG_LEDGER_ID, ts=20)
    freshness_checker.update_freshness(ledger_id=POOL_LEDGER_ID, ts=30)
    freshness_checker.check_freshness(10000)
    assert freshness_checker.get_last_update_time() == \
           OrderedDict([
               (DOMAIN_LEDGER_ID, 10),
               (CONFIG_LEDGER_ID, 20),
               (POOL_LEDGER_ID, 30)
           ])
