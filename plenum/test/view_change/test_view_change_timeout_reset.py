import pytest

from plenum.test.delayers import vcd_delay
from plenum.test.helper import waitForViewChange
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone

VIEW_CHANGE_TIMEOUT = 8
CATCHUP_TIMEOUT = 0.5 * VIEW_CHANGE_TIMEOUT


@pytest.fixture(scope="module")
def tconf(tconf):
    old_view_change_timeout = tconf.VIEW_CHANGE_TIMEOUT
    old_catchup_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    tconf.VIEW_CHANGE_TIMEOUT = VIEW_CHANGE_TIMEOUT
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = CATCHUP_TIMEOUT
    yield tconf
    tconf.VIEW_CHANGE_TIMEOUT = old_view_change_timeout
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = old_catchup_timeout


def test_view_change_timeout_reset_on_next_view(txnPoolNodeSet, looper, tconf):
    # Check that all nodes are in view 0
    assert all(n.viewNo == 0 for n in txnPoolNodeSet)

    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]
    with delay_rules(stashers, vcd_delay()):
        # Start first view change
        for n in txnPoolNodeSet:
            n.view_changer.on_master_degradation()
        waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1)
        looper.runFor(0.6 * VIEW_CHANGE_TIMEOUT)

        # Start second view change
        for n in txnPoolNodeSet:
            n.view_changer.on_master_degradation()
        waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=2)
        looper.runFor(0.6 * VIEW_CHANGE_TIMEOUT)

    # Ensure only 2 view changes happened
    ensureElectionsDone(looper, txnPoolNodeSet)
    for n in txnPoolNodeSet:
        assert n.viewNo == 2
