from collections import OrderedDict

import pytest

from plenum.test.delayers import ppDelay
from plenum.test.helper import freshness
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_view_changer_state_is_fresh_enough_when_all_update_times_are_within_timeout(tconf, txnPoolNodeSet, monkeypatch):
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
    current_view_no = max(node.viewNo for node in txnPoolNodeSet)
    for node in txnPoolNodeSet:
        assert node.viewNo == current_view_no

    looper.runFor(3 * FRESHNESS_TIMEOUT)

    for node in txnPoolNodeSet:
        assert node.viewNo == current_view_no

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)


def test_view_change_happens_if_ordering_is_halted(looper, tconf, txnPoolNodeSet,
                                                   sdk_wallet_client, sdk_pool_handle):
    current_view_no = max(node.viewNo for node in txnPoolNodeSet)
    for node in txnPoolNodeSet:
        assert node.viewNo == current_view_no

    def check_next_view():
        for node in txnPoolNodeSet:
            assert node.viewNo > current_view_no

    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]
    with delay_rules(stashers, ppDelay()):
        looper.run(eventually(check_next_view, timeout=FRESHNESS_TIMEOUT * 3))

    def has_freshness_instance_change(node):
        all_instance_changes = node.view_changer.spylog.getAll('sendInstanceChange')
        freshness_instance_changes = sum(1 for ic in all_instance_changes
                                      if ic.params['suspicion'].code == 43)
        return freshness_instance_changes > 0
    assert sum(1 for node in txnPoolNodeSet if has_freshness_instance_change(node)) >= 3

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
