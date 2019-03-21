from typing import Iterable

import pytest

from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import DEFAULT_DELAY, icDelay
from plenum.test.helper import max_3pc_batch_limits, sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import start_delaying, stop_delaying_and_process, delay_rules
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def delay_3pc_after(view_no, pp_seq_no, *args):
    if not args:
        args = (PrePrepare, Prepare, Commit)

    def _delayer(msg_frm):
        msg, frm = msg_frm
        if not isinstance(msg, args):
            return
        if compare_3PC_keys((view_no, pp_seq_no), (msg.viewNo, msg.ppSeqNo)) <= 0:
            return
        return DEFAULT_DELAY

    _delayer.__name__ = "delay_3pc_after({}, {}, {})".format(view_no, pp_seq_no, args)
    return _delayer


def check_nodes_ordered_till(nodes: Iterable, view_no: int, pp_seq_no: int):
    for node in nodes:
        assert compare_3PC_keys((view_no, pp_seq_no), node.master_replica.last_ordered_3pc) >= 0


def check_catchup_is_started(node):
    assert node.ledgerManager._node_leecher._state != NodeLeecherService.State.Idle


def test_view_change_during_unstash(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf):
    slow_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]

    slow_stasher = slow_node.nodeIbStasher
    other_stashers = [n.nodeIbStasher for n in other_nodes]
    all_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    # Preload nodes with some transactions
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    for node in txnPoolNodeSet:
        assert node.master_replica.last_ordered_3pc == (0, 1)

    # Stop ordering on slow node and send requests
    slow_node_remaining_commits = start_delaying(slow_stasher, delay_3pc_after(0, 7, Commit))
    slow_node_3pc = start_delaying(slow_stasher, delay_3pc_after(0, 0))
    reqs_view_0 = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 8)

    # Make pool order first 4 batches and pause
    pool_pause_3pc = start_delaying(other_stashers, delay_3pc_after(0, 5))
    looper.run(eventually(check_nodes_ordered_till, other_nodes, 0, 5))

    # Start catchup, continue ordering everywhere (except two last batches on slow node)
    slow_node._do_start_catchup(just_started=False)
    looper.run(eventually(check_catchup_is_started, slow_node))
    stop_delaying_and_process(slow_node_3pc)
    stop_delaying_and_process(pool_pause_3pc)
    looper.run(eventually(check_nodes_ordered_till, [slow_node], 0, 7))

    # Start view change and allow slow node to get remaining commits
    with delay_rules(all_stashers, icDelay()):
        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()
        looper.runFor(0.1)
    stop_delaying_and_process(slow_node_remaining_commits)

    # Ensure that everything is ok
    sdk_get_and_check_replies(looper, reqs_view_0)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
