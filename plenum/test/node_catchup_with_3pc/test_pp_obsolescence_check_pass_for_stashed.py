import pytest

from plenum.test import waits
from plenum.common.constants import LEDGER_STATUS, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import MessageReq, CatchupReq
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import ppDelay, pDelay, cDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.test_timestamp.helper import get_timestamp_suspicion_count
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules, start_delaying, stop_delaying_and_process
from stp_core.loop.eventually import eventually


# TODO better to use non hard coded value of nodes number
# but for now it would be tricky since tconf can't depend on txnPoolNodeSet

# should be big enough to pass PP during normal ordering flow
PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS = waits.expectedPrepareTime(4)


@pytest.fixture(scope="module")
def tconf(tconf):
    old_value = tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS
    yield tconf
    tconf.ACCEPTABLE_DEVIATION_PREPREPARE_SECS = old_value


def delay_domain_ledger_catchup():
    def delay(msg):
        msg = msg[0]
        delay = PATCHED_ACCEPTABLE_DEVIATION_PREPREPARE_SECS + 1
        if isinstance(msg, MessageReq) and \
                msg.msg_type == LEDGER_STATUS and \
                msg.params.get('ledgerId') == DOMAIN_LEDGER_ID:
            return delay
        if isinstance(msg, CatchupReq) and \
                msg.ledgerId == DOMAIN_LEDGER_ID:
            return delay

    return delay


def test_stashed_pp_pass_obsolescence_check(tdir, tconf,
                                     looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client):
    lagging_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]
    other_stashers = [node.nodeIbStasher for node in other_nodes]

    # TODO the test partly copies logic of catchup forcing from test_slow_catchup_while_ordering.py,
    # better to create a helper

    def lagging_node_state() -> NodeLeecherService.State:
        return lagging_node.ledgerManager._node_leecher._state

    def check_lagging_node_is_not_syncing_audit():
        assert lagging_node_state() != NodeLeecherService.State.SyncingAudit

    delay_handler = None
    # Prevent lagging node from ordering
    with delay_rules(lagging_node.nodeIbStasher, ppDelay(), pDelay(), cDelay()):
        # Order request on all nodes except lagging one
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)

        # Prevent lagging node from catching up domain ledger (and finishing catchup),
        # delay should be long enough to exceed PP ACCEPTABLE_DEVIATION_PREPREPARE_SECS
        delay_handler = start_delaying(other_stashers, delay_domain_ledger_catchup())

        # Start catchup on lagging node
        lagging_node.ledgerManager.start_catchup()
        assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

        # Ensure that audit ledger is caught up by lagging node
        looper.run(eventually(check_lagging_node_is_not_syncing_audit))
        assert lagging_node_state() != NodeLeecherService.State.Idle

        # Order one more request on all nodes except lagging one
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)

    # Now delayed 3PC messages reach lagging node, but goes to stahed state since
    # catch up is still not finished. Here timestamps for PP are set.

    # Stop catchup delaying to allow lagging node catching up domain ledger
    # which contains more transactions than it was when audit ledger was caught up,
    # delayed 3PC messages are still in stashed state
    stop_delaying_and_process(delay_handler)

    # Now delayed 3PC messages are unstanshed and can be ordered,
    # ensure that all nodes will have same data after that
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # ensure that no suspicions about obsolete PP have been raised
    assert get_timestamp_suspicion_count(lagging_node) == 0
