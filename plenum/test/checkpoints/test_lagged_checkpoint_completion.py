from plenum.test import waits
from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from stp_core.loop.eventually import eventually

CHK_FREQ = 5


def test_lagged_checkpoint_completion(chkFreqPatched, looper, txnPoolNodeSet,
                                      sdk_wallet_client, sdk_pool_handle):
    slow_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 4)

    slow_node.nodeIbStasher.delay(cDelay())

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    def check():
        for replica in slow_node.replicas:
            assert len(replica.checkpoints) == 1
            assert (1, 5) in replica.checkpoints
            assert replica.checkpoints[(1, 5)].isStable is False

            assert len(replica.stashedRecvdCheckpoints) == 1
            assert 0 in replica.stashedRecvdCheckpoints
            assert len(replica.stashedRecvdCheckpoints[0]) == 1
            assert (1, 5) in replica.stashedRecvdCheckpoints[0]
            assert len(replica.stashedRecvdCheckpoints[0][(1, 5)]) == \
                len(other_nodes)

    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(check, timeout=stabilization_timeout))

    slow_node.nodeIbStasher.reset_delays_and_process_delayeds()

    waitNodeDataEquality(looper, slow_node, *other_nodes,
                         customTimeout=waits.expectedOrderingTime(
                             len(txnPoolNodeSet)))

    for replica in slow_node.replicas:
        assert len(replica.checkpoints) == 1
        assert (1, 5) in replica.checkpoints
        assert replica.checkpoints[(1, 5)].isStable is True

        assert len(replica.stashedRecvdCheckpoints) == 0
