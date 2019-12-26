import pytest

from plenum.test.delayers import ppDelay, icDelay
from plenum.test.freshness.helper import has_freshness_instance_change
from plenum.test.helper import freshness
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_freshness_instance_changes_are_sent_continuosly(looper, tconf, txnPoolNodeSet,
                                                         sdk_wallet_client, sdk_pool_handle):
    current_view_no = txnPoolNodeSet[0].viewNo
    for node in txnPoolNodeSet:
        assert node.viewNo == current_view_no

    def check_instance_change_messages(count=1):
        for node in txnPoolNodeSet:
            assert has_freshness_instance_change(node, count)

    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]
    with delay_rules(stashers, ppDelay(), icDelay()):
        looper.run(eventually(check_instance_change_messages, 3, timeout=FRESHNESS_TIMEOUT * 5))

        for node in txnPoolNodeSet:
            all_instance_changes = node.master_replica.\
                _view_change_trigger_service.spylog.getAll('_send_instance_change')
            freshness_instance_changes = [ic for ic in all_instance_changes
                                          if ic.params['suspicion'].code == 43]

            # Ensure that all instance change messages were due to freshness
            assert len(all_instance_changes) == len(freshness_instance_changes)

            # Ensure that all instance change messages are for same view
            for ic in freshness_instance_changes:
                assert ic.params['view_no'] == current_view_no + 1

            # Ensure instance change messages had sensible interval
            for ic1, ic2 in zip(freshness_instance_changes[1:], freshness_instance_changes):
                interval = ic2.starttime - ic1.starttime
                assert 0.9 * FRESHNESS_TIMEOUT < interval < 2.1 * FRESHNESS_TIMEOUT
