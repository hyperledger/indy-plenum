import pytest

from plenum.test import waits
from plenum.common.constants import LEDGER_STATUS, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import MessageReq, CatchupReq
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import ppDelay, pDelay, cDelay, DEFAULT_DELAY
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.test_timestamp.helper import get_timestamp_suspicion_count
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules, start_delaying, stop_delaying_and_process
from stp_core.loop.eventually import eventually


def delay_domain_ledger_catchup():
    def delay(msg):
        msg = msg[0]
        if isinstance(msg, MessageReq) and \
                msg.msg_type == LEDGER_STATUS and \
                msg.params.get('ledgerId') == DOMAIN_LEDGER_ID:
            return DEFAULT_DELAY
        if isinstance(msg, CatchupReq) and \
                msg.ledgerId == DOMAIN_LEDGER_ID:
            return DEFAULT_DELAY

    return delay


def test_first_audit_catchup_during_ordering(tdir, tconf, looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    lagging_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]
    other_stashers = [node.nodeIbStasher for node in other_nodes]

    def lagging_node_state() -> NodeLeecherService.State:
        return lagging_node.ledgerManager._node_leecher._state

    def check_lagging_node_is_not_syncing_audit():
        assert lagging_node_state() != NodeLeecherService.State.SyncingAudit

    # Prevent lagging node from catching up domain ledger (and finishing catchup)
    with delay_rules(other_stashers, delay_domain_ledger_catchup()):
        # Start catchup on lagging node
        lagging_node.start_catchup()
        assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

        # Ensure that audit ledger is caught up by lagging node
        looper.run(eventually(check_lagging_node_is_not_syncing_audit))
        assert lagging_node_state() != NodeLeecherService.State.Idle

        # Order request on all nodes except lagging one where they goes to stashed state
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)

    # Now catchup should end and lagging node starts processing stashed PPs
    # and resumes ordering

    # ensure that all nodes will have same data after that
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # ensure that no suspicions about obsolete PP have been raised
    assert get_timestamp_suspicion_count(lagging_node) == 0
