import types
import pytest

from plenum.test.view_change.helper import ensure_view_change_complete


@pytest.fixture(scope="module")
def tconf(tconf):
    old_view_change_timeout = tconf.VIEW_CHANGE_TIMEOUT
    old_catchup_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    tconf.VIEW_CHANGE_TIMEOUT = 10
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = 5
    yield tconf
    tconf.VIEW_CHANGE_TIMEOUT = old_view_change_timeout
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = old_catchup_timeout


def test_view_change_can_finish_despite_perpetual_catchup(txnPoolNodeSet, looper, tconf):
    # Make nodes think that there is perpetual catchup
    old_methods = [n.num_txns_caught_up_in_last_catchup for n in txnPoolNodeSet]
    for n in txnPoolNodeSet:
        n.num_txns_caught_up_in_last_catchup = types.MethodType(lambda _n: 10, n)

    ensure_view_change_complete(looper, txnPoolNodeSet)

    # Restore node behaviour
    for n, old_method in zip(txnPoolNodeSet, old_methods):
        n.num_txns_caught_up_in_last_catchup = old_method
