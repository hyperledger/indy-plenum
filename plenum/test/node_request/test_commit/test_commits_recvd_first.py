from plenum.common.util import check_if_all_equal_in_list
from plenum.test.delayers import ppDelay, pDelay
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.pool_transactions.conftest import looper
from plenum.test.helper import sdk_send_batches_of_random_and_check


def test_commits_recvd_first(looper, txnPoolNodeSet,
                             sdk_wallet_client, sdk_pool_handle):
    slow_node = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet, 0)][-1]
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    delay = 50
    slow_node.nodeIbStasher.delay(ppDelay(delay, 0))
    slow_node.nodeIbStasher.delay(pDelay(delay, 0))

    sdk_send_batches_of_random_and_check(looper,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         num_reqs=20,
                                         num_batches=4)

    assert not slow_node.master_replica.prePrepares
    assert not slow_node.master_replica.prepares
    assert not slow_node.master_replica.commits
    assert len(slow_node.master_replica.commitsWaitingForPrepare) > 0

    slow_node.reset_delays_and_process_delayeds()
    waitNodeDataEquality(looper, slow_node, *other_nodes)
    assert check_if_all_equal_in_list([n.master_replica.ordered
                                       for n in txnPoolNodeSet])

    assert slow_node.master_replica.prePrepares
    assert slow_node.master_replica.prepares
    assert slow_node.master_replica.commits
    assert not slow_node.master_replica.commitsWaitingForPrepare
