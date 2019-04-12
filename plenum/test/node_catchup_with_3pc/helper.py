from typing import Iterable

from plenum.common.messages.node_messages import Commit
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import delay_3pc, cr_delay
from plenum.test.helper import sdk_send_random_pool_requests, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually


def delay_catchup(ledger_id: int):
    _delayer = cr_delay(ledger_filter=ledger_id)
    _delayer.__name__ = "delay_catchup({})".format(ledger_id)
    return _delayer


def check_catchup_with_skipped_commits_received_before_catchup(ledger_id,
                                                               looper,
                                                               txnPoolNodeSet,
                                                               sdk_pool_handle,
                                                               sdk_wallet_new_steward):
    lagging_node = txnPoolNodeSet[-1]
    lagging_stasher = lagging_node.nodeIbStasher
    other_nodes = txnPoolNodeSet[:-1]

    def check_lagging_node_done_catchup():
        assert lagging_node.ledgerManager._node_leecher._state == NodeLeecherService.State.Idle

    def check_nodes_ordered_till(nodes: Iterable, view_no: int, pp_seq_no: int):
        for node in nodes:
            assert compare_3PC_keys((view_no, pp_seq_no), node.master_replica.last_ordered_3pc) >= 0

    # Make sure pool is in expected state
    init_pp_seq_no = txnPoolNodeSet[0].master_replica.last_ordered_3pc[1]
    for node in txnPoolNodeSet:
        assert node.master_replica.last_ordered_3pc == (0, init_pp_seq_no)

    # Order pool requests while delaying first two commits on lagging node
    with delay_rules(lagging_stasher, delay_3pc(before=init_pp_seq_no + 3, msgs=Commit)):
        # Send some pool requests
        reqs = sdk_send_random_pool_requests(looper, sdk_pool_handle, sdk_wallet_new_steward, 4)
        sdk_get_and_check_replies(looper, reqs)

    # Make sure pool is in expected state
    for node in other_nodes:
        assert node.master_replica.last_ordered_3pc == (0, init_pp_seq_no + 4)
    assert lagging_node.master_replica.last_ordered_3pc == (0, init_pp_seq_no)

    # Wait until two batches with delayed commits are ordered, but not more
    looper.run(eventually(check_nodes_ordered_till, [lagging_node], 0, init_pp_seq_no + 2))
    assert lagging_node.master_replica.last_ordered_3pc == (0, init_pp_seq_no + 2)

    with delay_rules(lagging_stasher, delay_catchup(ledger_id)):
        # Start catchup
        lagging_node.start_catchup()

        # Give a chance for process_stashed_out_of_order_commits to fire before POOL_LEDGER is actually caught up
        looper.runFor(3.0)

    # Ensure that audit ledger is caught up by lagging node
    looper.run(eventually(check_lagging_node_done_catchup))

    # Ensure that all nodes will eventually have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
