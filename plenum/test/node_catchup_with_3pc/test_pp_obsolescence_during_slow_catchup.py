import pytest
import asyncio

from plenum.common.constants import LEDGER_STATUS, DOMAIN_LEDGER_ID, CATCHUP_REQ
from plenum.common.messages.node_messages import MessageReq, CatchupReq
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import ppDelay, pDelay, cDelay, DEFAULT_DELAY
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.test_timestamp.helper import get_timestamp_suspicion_count
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually


PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS = 3  # TODO lower / higher value ?


@pytest.fixture(scope="module")
def tconf(tconf):
    old_value = tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    yield tconf
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = old_value


def test_pp_obsolescence_for_lagging_node(tdir, tconf,
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
        # TODO use some of waits instead
        looper.run(asyncio.sleep(delay))

    # Now delayed 3PC messages reach lagging node, so any transactions missed during
    # catch up can be ordered, ensure that all nodes will have same data after that
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    assert get_timestamp_suspicion_count(lagging_node) == 0
