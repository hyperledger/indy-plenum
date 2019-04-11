from logging import getLogger
from typing import List, Iterable

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, AUDIT_LEDGER_ID, PREPREPARE, PREPARE, COMMIT, POOL_LEDGER_ID
from plenum.common.messages.node_messages import Commit
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import cr_delay, delay_3pc, msg_rep_delay
from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_requests, sdk_get_and_check_replies, \
    max_3pc_batch_limits, sdk_send_random_pool_requests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules, start_delaying, stop_delaying_and_process
from stp_core.loop.eventually import eventually

logger = getLogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def test_catchup_with_skipped_commits_received_before_catchup(tdir, tconf,
                                                              looper,
                                                              txnPoolNodeSet,
                                                              sdk_pool_handle,
                                                              sdk_wallet_new_steward):
    lagging_node = txnPoolNodeSet[-1]
    lagging_stasher = lagging_node.nodeIbStasher
    other_nodes = txnPoolNodeSet[:-1]

    def lagging_node_state() -> NodeLeecherService.State:
        return lagging_node.ledgerManager._node_leecher._state

    def check_lagging_node_is_not_syncing_audit():
        assert lagging_node_state() != NodeLeecherService.State.SyncingAudit

    def check_lagging_node_done_catchup():
        assert lagging_node_state() == NodeLeecherService.State.Idle

    def check_nodes_ordered_till(nodes: Iterable, view_no: int, pp_seq_no: int):
        for node in nodes:
            assert compare_3PC_keys((view_no, pp_seq_no), node.master_replica.last_ordered_3pc) >= 0

    # Make sure pool is in expected state
    for node in txnPoolNodeSet:
        assert node.master_replica.last_ordered_3pc == (0, 1)

    # Order pool requests while delaying first two commits on lagging node
    with delay_rules(lagging_stasher, delay_3pc(before=4, msgs=Commit)):
        # Send some pool requests
        reqs = sdk_send_random_pool_requests(looper, sdk_pool_handle, sdk_wallet_new_steward, 4)
        sdk_get_and_check_replies(looper, reqs)

    # Make sure pool is in expected state
    for node in other_nodes:
        assert node.master_replica.last_ordered_3pc == (0, 5)
    assert lagging_node.master_replica.last_ordered_3pc == (0, 1)

    # Wait until two batches with delayed commits are ordered
    looper.run(eventually(check_nodes_ordered_till, [lagging_node], 0, 3))
    assert lagging_node.master_replica.last_ordered_3pc == (0, 3)

    with delay_rules(lagging_stasher, delay_catchup(POOL_LEDGER_ID)):
        with delay_rules(lagging_stasher, delay_catchup(AUDIT_LEDGER_ID)):
            # Start catchup
            lagging_node.start_catchup()
            assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

            # Give a chance for process_stashed_out_of_order_commits to fire
            looper.runFor(5.0)

    # Ensure that audit ledger is caught up by lagging node
    looper.run(eventually(check_lagging_node_done_catchup))

    # Ensure that all nodes will eventually have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


def delay_catchup(ledger_id: int):
    _delayer = cr_delay(ledger_filter=ledger_id)
    _delayer.__name__ = "delay_catchup({})".format(ledger_id)
    return _delayer
