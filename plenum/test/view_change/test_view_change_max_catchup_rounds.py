from plenum.common.util import check_if_all_equal_in_list
from plenum.test.delayers import pDelay, cDelay
from plenum.test.helper import sdk_send_batches_of_random_and_check, sdk_send_random_requests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import getNonPrimaryReplicas, ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change


def test_view_change_after_max_catchup_rounds(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    The node should do only a fixed rounds of catchup. For this delay Prepares
    and Commits for 2 non-primary nodes by a large amount which is equivalent
    to loss of Prepares and Commits. Make sure 2 nodes have a different last
    prepared certificate from other two. Then do a view change, make sure view
    change completes and the pool does not process the request that were
    prepared by only a subset of the nodes
    """
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, 2 * 3, 3)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    ledger_summary = txnPoolNodeSet[0].ledger_summary

    slow_nodes = [r.node for r in getNonPrimaryReplicas(
        txnPoolNodeSet, 0)[-2:]]
    fast_nodes = [n for n in txnPoolNodeSet if n not in slow_nodes]

    # Make node slow to process Prepares and Commits
    for node in slow_nodes:
        node.nodeIbStasher.delay(pDelay(120, 0))
        node.nodeIbStasher.delay(cDelay(120, 0))

    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 5)
    looper.runFor(3)

    ensure_view_change(looper, nodes=txnPoolNodeSet)

    def last_prepared(nodes):
        lst = [n.master_replica.last_prepared_certificate_in_view()
               for n in nodes]
        # All nodes have same last prepared
        assert check_if_all_equal_in_list(lst)
        return lst[0]

    last_prepared_slow = last_prepared(slow_nodes)
    last_prepared_fast = last_prepared(fast_nodes)

    # Check `slow_nodes` and `fast_nodes` set different last_prepared
    assert last_prepared_fast != last_prepared_slow

    # View change complete
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # The requests which were prepared by only a subset of the nodes were
    # not ordered
    assert txnPoolNodeSet[0].ledger_summary == ledger_summary

    for node in slow_nodes:
        node.nodeIbStasher.reset_delays_and_process_delayeds()

    # Make sure pool is functional
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, 10, 2)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    last_prepared(txnPoolNodeSet)
