import pytest
import asyncio

from plenum.test import waits
from plenum.test.delayers import ppDelay, pDelay, cDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.test_timestamp.helper import get_timestamp_suspicion_count
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.spy_helpers import get_count
from stp_core.loop.eventually import eventually

nodeCount = 4

# should be big enough to pass PP during normal ordering flow
PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS = waits.expectedPrepareTime(nodeCount)


@pytest.fixture(scope="module")
def tconf(tconf):
    old_value = tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    yield tconf
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = old_value


# TODO this test should actually fail someday when ts for PP
# is set before replica level processing (e.g. in zstack)
def test_pp_obsolescence_check_fail_for_delayed(tdir, tconf,
                                     looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client):

    delay = PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS + 1
    lagging_node = txnPoolNodeSet[-1]

    # Prevent lagging node from ordering
    with delay_rules(
        lagging_node.nodeIbStasher, ppDelay(), pDelay(), cDelay()
    ):
        # Order request on all nodes except lagging one
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)
        looper.run(asyncio.sleep(delay))

    # Now delayed 3PC messages reach lagging node, so any delayed transactions
    # can be processed (PrePrepare would be discarded but requested after that),
    # ensure that all nodes will have same data after that
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    pp_count = get_count(lagging_node.master_replica._ordering_service,
                         lagging_node.master_replica._ordering_service.process_preprepare)

    assert pp_count > 0
    assert get_timestamp_suspicion_count(lagging_node) == 1
