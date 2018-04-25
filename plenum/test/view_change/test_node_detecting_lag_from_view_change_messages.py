import types

import pytest

from plenum.common.util import compare_3PC_keys
from plenum.test.delayers import delay_3pc_messages, icDelay, cDelay
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, \
    sdk_send_random_requests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

TestRunningTimeLimitSec = 150


@pytest.mark.skip(reason='Pending complete implementation')
def test_node_detecting_lag_from_view_change_done_messages(txnPoolNodeSet,
                                                           looper,
                                                           sdk_pool_handle,
                                                           sdk_wallet_client,
                                                           tconf):
    """
    A node is slow and after view change starts, it marks it's `last_prepared`
    to less than others, after catchup it does not get any txns from others
    and finds it has already ordered it's `last_prepared`, but when
    it gets ViewChangeDone messages, it starts catchup again and this
    time gets the txns. To achieve this delay all 3PC messages to a node so
    before view change it has different last_prepared from others.
    Also delay processing of COMMITs and INSTANCE_CHANGEs by other nodes
    """
    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           2 * 3,
                                           3)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    fast_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    slow_master_replica = slow_node.master_replica
    fast_master_replicas = [n.master_replica for n in fast_nodes]

    delay_3pc = 50
    delay_ic = tconf.PerfCheckFreq + 5
    delay_commit = delay_ic + 10
    delay_3pc_messages([slow_node], 0, delay_3pc)
    for n in fast_nodes:
        n.nodeIbStasher.delay(icDelay(delay_ic))
        n.nodeIbStasher.delay(cDelay(delay_commit))

    reqs = []
    for i in range(10):
        # fix if unskip
        reqs = reqs + sdk_send_random_requests()
        looper.runFor(.2)

    def chk1():
        for rep in fast_master_replicas:
            assert compare_3PC_keys(
                slow_master_replica.last_prepared_certificate_in_view(),
                rep.last_prepared_certificate_in_view()) > 0
            assert slow_master_replica.last_ordered_3pc == rep.last_ordered_3pc

    looper.run(eventually(chk1))

    no_more_catchup_count = get_count(slow_node,
                                      slow_node.no_more_catchups_needed)

    # Track last prepared for master replica of each node
    prepareds = {}
    orig_methods = {}
    for node in txnPoolNodeSet:
        orig_methods[node.name] = node.master_replica.on_view_change_start

        def patched_on_view_change_start(self):
            orig_methods[self.node.name]()
            prepareds[self.node.name] = self.last_prepared_before_view_change

        node.master_replica.on_view_change_start = types.MethodType(
            patched_on_view_change_start, node.master_replica)

    ensure_view_change(looper, txnPoolNodeSet, exclude_from_check=fast_nodes)

    def chk2():
        # last_prepared of slow_node is less than fast_nodes
        for rep in fast_master_replicas:
            assert compare_3PC_keys(prepareds[slow_master_replica.node.name],
                                    prepareds[rep.node.name]) > 0

    looper.run(eventually(chk2, timeout=delay_ic + 5))

    last_start_catchup_call_at = None
    no_more_catchup_call_at = None

    def chk3():
        # no_more_catchups_needed was called since node found no need of
        # catchup
        nonlocal last_start_catchup_call_at, no_more_catchup_call_at
        assert (get_count(slow_node, slow_node.no_more_catchups_needed) -
                no_more_catchup_count) > 0

        no_more_catchup_call_at = slow_node.spylog.getLast(
            slow_node.no_more_catchups_needed).starttime
        last_start_catchup_call_at = slow_node.spylog.getLast(
            slow_node.start_catchup).starttime

    looper.run(eventually(chk3, timeout=delay_commit))

    for n in fast_nodes:
        n.nodeIbStasher.reset_delays_and_process_delayeds()
        n.nodeIbStasher.reset_delays_and_process_delayeds()

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    assert slow_node.spylog.getLast(
        slow_node.start_catchup).starttime > no_more_catchup_call_at
    assert slow_node.spylog.getLast(
        slow_node.start_catchup).starttime > last_start_catchup_call_at
