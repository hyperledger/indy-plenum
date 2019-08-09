from plenum.common.constants import PREPREPARE, PREPARE
from plenum.common.util import check_if_all_equal_in_list
from plenum.test.delayers import ppDelay, pDelay, msg_rep_delay
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.helper import sdk_send_batches_of_random_and_check


def test_dequeue_and_validate_commits(looper, txnPoolNodeSet,
                                      sdk_wallet_client, sdk_pool_handle):
    slow_node = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet, 0)][-1]
    other_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    delay = 50
    with delay_rules(slow_node.nodeIbStasher,
                     pDelay(delay),
                     msg_rep_delay(delay, [PREPARE, PREPREPARE])):
        with delay_rules(slow_node.nodeIbStasher, ppDelay(delay)):

            sdk_send_batches_of_random_and_check(looper,
                                                 txnPoolNodeSet,
                                                 sdk_pool_handle,
                                                 sdk_wallet_client,
                                                 num_reqs=1,
                                                 num_batches=1)

            assert not slow_node.master_replica._ordering_service.prePrepares
            assert not slow_node.master_replica._ordering_service.prepares
            assert not slow_node.master_replica._ordering_service.commits
            assert len(slow_node.master_replica._ordering_service.commitsWaitingForPrepare) > 0

        waitNodeDataEquality(looper, slow_node, *other_nodes)
        assert check_if_all_equal_in_list([n.master_replica._ordering_service.ordered
                                           for n in txnPoolNodeSet])

        assert slow_node.master_replica._ordering_service.prePrepares
        assert slow_node.master_replica._ordering_service.prepares
        assert slow_node.master_replica._ordering_service.commits
        assert not slow_node.master_replica._ordering_service.commitsWaitingForPrepare

        assert all(slow_node.master_replica.last_ordered_3pc == n.master_replica.last_ordered_3pc
                   for n in other_nodes)
