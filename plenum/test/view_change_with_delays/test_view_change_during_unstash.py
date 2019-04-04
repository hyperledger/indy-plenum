from typing import Iterable

import pytest

from plenum.common.messages.node_messages import Prepare, Commit
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import icDelay, cr_delay, delay_3pc
from plenum.test.helper import max_3pc_batch_limits, sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_replies, sdk_check_reply
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import start_delaying, stop_delaying_and_process, delay_rules
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def check_nodes_ordered_till(nodes: Iterable, view_no: int, pp_seq_no: int):
    for node in nodes:
        assert compare_3PC_keys((view_no, pp_seq_no), node.master_replica.last_ordered_3pc) >= 0


def check_catchup_is_started(node):
    assert node.ledgerManager._node_leecher._state != NodeLeecherService.State.Idle


def check_catchup_is_finished(node):
    assert node.ledgerManager._node_leecher._state == NodeLeecherService.State.Idle


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

    # Prevent ordering of some requests
    start_delaying(all_stashers, delay_3pc(after=7, msgs=(Prepare, Commit)))

    # Stop ordering on slow node and send requests
    slow_node_after_5 = start_delaying(slow_stasher, delay_3pc(after=5, msgs=Commit))
    slow_node_until_5 = start_delaying(slow_stasher, delay_3pc(after=0))
    reqs_view_0 = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 8)

    # Make pool order first 2 batches and pause
    pool_after_3 = start_delaying(other_stashers, delay_3pc(after=3))
    looper.run(eventually(check_nodes_ordered_till, other_nodes, 0, 3))

    # Start catchup, continue ordering everywhere (except two last batches on slow node)
    with delay_rules(slow_stasher, cr_delay()):
        slow_node._do_start_catchup(just_started=False)
        looper.run(eventually(check_catchup_is_started, slow_node))
        stop_delaying_and_process(pool_after_3)
        looper.run(eventually(check_nodes_ordered_till, other_nodes, 0, 7))

    # Finish catchup and continue processing on slow node
    looper.run(eventually(check_catchup_is_finished, slow_node))
    stop_delaying_and_process(slow_node_until_5)
    looper.run(eventually(check_nodes_ordered_till, [slow_node], 0, 5))

    # Start view change and allow slow node to get remaining commits
    with delay_rules(all_stashers, icDelay()):
        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()
        looper.runFor(0.1)
    stop_delaying_and_process(slow_node_after_5)

    # Ensure that expected number of requests was ordered
    replies = sdk_get_replies(looper, reqs_view_0)
    for rep in replies[:6]:
        sdk_check_reply(rep)

    # Ensure that everything is ok
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
