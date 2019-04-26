import pytest

from plenum.test.delayers import cr_delay, cDelay
from plenum.test.helper import sdk_send_random_and_check, freshness
from plenum.test.stasher import delay_rules

STATE_FRESHNESS_UPDATE_INTERVAL = 5
ACCEPTABLE_FRESHNESS_INTERVALS_COUNT = 1


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=STATE_FRESHNESS_UPDATE_INTERVAL):
        old_intervals_count = tconf.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT
        tconf.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT = ACCEPTABLE_FRESHNESS_INTERVALS_COUNT
        yield tconf
        tconf.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT = old_intervals_count


def test_freshness_in_catchup(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward,
                              tconf, ):
    """
    Check that InstanceChange with reason "State signatures are not updated for too long"
    are not sends in catchup without view change.
    """
    view_no = txnPoolNodeSet[0].viewNo

    lagging_node = txnPoolNodeSet[-1]

    with delay_rules(lagging_node.nodeIbStasher, cr_delay(), cDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 1)
        lagging_node.start_catchup()
        looper.runFor(tconf.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT * tconf.STATE_FRESHNESS_UPDATE_INTERVAL + 5)

    print(lagging_node.view_changer.instance_changes._cache)
    assert not lagging_node.view_changer.instance_changes.has_inst_chng_from(view_no + 1, lagging_node.name)
