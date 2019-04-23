from logging import getLogger

import pytest

from plenum.common.constants import LEDGER_STATUS, DOMAIN_LEDGER_ID, CATCHUP_REQ
from plenum.common.messages.node_messages import MessageReq, CatchupReq
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import ppDelay, pDelay, cDelay, DEFAULT_DELAY
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually

logger = getLogger()

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


def test_slow_catchup_while_ordering(tdir, tconf,
                                     looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client):
    lagging_node = txnPoolNodeSet[-1]
    other_lagging_node = txnPoolNodeSet[-2]
    other_nodes = txnPoolNodeSet[:-1]
    other_stashers = [node.nodeIbStasher for node in other_nodes]

    def lagging_node_state() -> NodeLeecherService.State:
        return lagging_node.ledgerManager._node_leecher._state

    def check_lagging_node_is_not_syncing_audit():
        assert lagging_node_state() != NodeLeecherService.State.SyncingAudit

    # Prevent lagging node from ordering
    with delay_rules(lagging_node.nodeIbStasher, ppDelay(), pDelay(), cDelay()):
        # Order request on all nodes except lagging one
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)

        # Prevent lagging node from catching up domain ledger (and finishing catchup)
        with delay_rules(other_stashers, delay_domain_ledger_catchup()):
            # Start catchup on lagging node
            lagging_node.ledgerManager.start_catchup()
            assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

            # Ensure that audit ledger is caught up by lagging node
            looper.run(eventually(check_lagging_node_is_not_syncing_audit))
            assert lagging_node_state() != NodeLeecherService.State.Idle

            # Order one more request on all nodes except lagging one
            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_client, 1)

        # Now lagging node can catch up domain ledger which contains more transactions
        # than it was when audit ledger was caught up

    # Now delayed 3PC messages reach lagging node, so any transactions missed during
    # catch up can be ordered, ensure that all nodes will have same data after that
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # Ensure that even if we disable some other node pool is still functional
    # (it won't be the case if old lagging node is nonfunctional)
    with delay_rules(other_lagging_node.nodeIbStasher, ppDelay(), pDelay(), cDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 1)

    # Ensure that all nodes will eventually have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
