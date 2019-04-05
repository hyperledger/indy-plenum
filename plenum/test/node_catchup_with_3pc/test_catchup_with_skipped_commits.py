from logging import getLogger
from typing import List

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.common.messages.node_messages import Commit
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import DEFAULT_DELAY, cr_delay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules, start_delaying, stop_delaying_and_process
from stp_core.loop.eventually import eventually

logger = getLogger()


def delay_some_commits(view_no: int, pp_seq_nos: List[int]):
    def _delayer(msg_frm):
        msg, frm = msg_frm
        if not isinstance(msg, Commit):
            return
        if msg.viewNo != view_no:
            return
        if msg.ppSeqNo not in pp_seq_nos:
            return
        return DEFAULT_DELAY

    _delayer.__name__ = "delay_some_commits({}, {})".format(view_no, pp_seq_nos)
    return _delayer


def delay_catchup(ledger_id: int):
    _delayer = cr_delay(ledger_filter=ledger_id)
    _delayer.__name__ = "delay_catchup({})".format(ledger_id)
    return _delayer


def test_catchup_with_skipped_commits(tdir, tconf,
                                      looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client):
    lagging_node = txnPoolNodeSet[-1]
    lagging_stasher = lagging_node.nodeIbStasher
    other_nodes = txnPoolNodeSet[:-1]
    other_stashers = [node.nodeIbStasher for node in other_nodes]

    def lagging_node_state() -> NodeLeecherService.State:
        return lagging_node.ledgerManager._node_leecher._state

    def check_lagging_node_is_not_syncing_audit():
        assert lagging_node_state() != NodeLeecherService.State.SyncingAudit

    def check_lagging_node_done_catchup():
        assert lagging_node_state() == NodeLeecherService.State.Idle

    # Preload nodes with some transactions
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    for node in txnPoolNodeSet:
        assert node.master_replica.last_ordered_3pc == (0, 1)

    # Delay some commits on lagging node
    some_commits = start_delaying(lagging_stasher, delay_some_commits(0, [2, 3]))

    # Order 4 more requests in pool
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 4)

    with delay_rules(lagging_stasher, delay_catchup(AUDIT_LEDGER_ID)):
        # Start catchup
        lagging_node.start_catchup()
        looper.runFor(0.5)
        assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

        # Allow some missing commits to be finally received
        stop_delaying_and_process(some_commits)
        looper.runFor(0.5)

    # Ensure that audit ledger is caught up by lagging node
    looper.run(eventually(check_lagging_node_done_catchup))

    # Ensure that all nodes will eventually have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
