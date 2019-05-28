from collections import OrderedDict

import pytest

from plenum.common.startable import Mode
from plenum.test.helper import freshness
from plenum.test.node_request.helper import sdk_ensure_pool_functional

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_view_changer_state_is_fresh_enough_when_all_update_times_are_within_timeout(tconf, txnPoolNodeSet,
                                                                                     monkeypatch):
    node = txnPoolNodeSet[0]

    monkeypatch.setattr(node.master_replica, 'get_time_for_3pc_batch',
                        lambda: 16)
    monkeypatch.setattr(node.master_replica, 'get_ledgers_last_update_time',
                        lambda: OrderedDict([(0, 11), (2, 13), (1, 15)]))

    assert node.view_changer.is_state_fresh_enough()


def test_view_changer_state_is_not_fresh_enough_when_any_update_time_is_too_old(tconf, txnPoolNodeSet, monkeypatch):
    node = txnPoolNodeSet[0]

    monkeypatch.setattr(node.master_replica, 'get_time_for_3pc_batch',
                        lambda: 16)
    monkeypatch.setattr(node.master_replica, 'get_ledgers_last_update_time',
                        lambda: OrderedDict([(0, 5), (2, 10), (1, 15)]))

    assert not node.view_changer.is_state_fresh_enough()


def test_new_node_view_changer_state_is_fresh_enough(tconf, txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    assert node.view_changer.is_state_fresh_enough()


def test_view_change_doesnt_happen_if_pool_is_left_alone(looper, tconf, txnPoolNodeSet,
                                                         sdk_wallet_client, sdk_pool_handle):
    current_view_no = txnPoolNodeSet[0].viewNo
    for node in txnPoolNodeSet:
        assert node.viewNo == current_view_no

    looper.runFor(3 * FRESHNESS_TIMEOUT)

    for node in txnPoolNodeSet:
        assert node.viewNo == current_view_no

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_view_changer_state_is_not_fresh_in_view_change(tconf, txnPoolNodeSet, monkeypatch):
    node = txnPoolNodeSet[0]

    monkeypatch.setattr(node.master_replica, 'get_time_for_3pc_batch',
                        lambda: 16)
    monkeypatch.setattr(node.master_replica, 'get_ledgers_last_update_time',
                        lambda: OrderedDict([(0, 5), (2, 10), (1, 15)]))
    monkeypatch.setattr(node.view_changer.provider, 'node_mode',
                        lambda: Mode.starting)
    node.view_changer.view_change_in_progress = True

    assert not node.view_changer.is_state_fresh_enough()
    node.view_changer.in_progress = False
