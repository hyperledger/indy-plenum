from plenum.common.messages.internal_messages import NeedViewChange
from plenum.common.util import getMaxFailures
from plenum.server.consensus.primary_selector import RoundRobinPrimariesSelector
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone


REQ_COUNT = 10


def trigger_view_change(txnPoolNodeSet, proposed_view_no):
    for n in txnPoolNodeSet:
        replica = n.master_replica
        replica.internal_bus.send(NeedViewChange(proposed_view_no))
        assert replica._consensus_data.waiting_for_new_view


def get_next_primary(txnPoolNodeSet, expected_view_no):
    selector = RoundRobinPrimariesSelector()
    inst_count = len(txnPoolNodeSet[0].replicas)
    next_p_name = selector.select_primaries(txnPoolNodeSet, inst_count, expected_view_no)[0]
    return [n.name == next_p_name for n in txnPoolNodeSet][0]


def test_view_change_triggered(looper, txnPoolNodeSet):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    trigger_view_change(txnPoolNodeSet, current_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)


def test_view_change_triggered_after_ordering(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, REQ_COUNT)
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    trigger_view_change(txnPoolNodeSet, current_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)


def test_stopping_next_primary(looper, txnPoolNodeSet):
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    next_primary = get_next_primary(txnPoolNodeSet, old_view_no + 1)
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, next_primary)
    trigger_view_change(txnPoolNodeSet, old_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    assert current_view_no == old_view_no + 2
