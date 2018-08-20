from plenum.test.helper import sdk_send_random_and_check, sdk_send_batches_of_random_and_check
from plenum.test.malicious_behaviors_node import delaysPrePrepareProcessing, \
    changesRequest, dontSendPrepareAndCommitTo, resetSendPrepareAndCommitTo


def test_1_node_get_only_preprepare(looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client):
    master_node = txnPoolNodeSet[0]
    behind_node = txnPoolNodeSet[-1]
    num_of_batches = 3

    # Every node order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert behind_node.master_last_ordered_3PC == \
           master_node.master_last_ordered_3PC

    # Emulate connection problems, receiving only pre-prepares
    dontSendPrepareAndCommitTo(txnPoolNodeSet[:-1], txnPoolNodeSet[-1].name)

    # Send some txns and last node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert behind_node.master_last_ordered_3PC[1] + num_of_batches == \
           master_node.master_last_ordered_3PC[1]

    # Remove connection problems
    resetSendPrepareAndCommitTo(txnPoolNodeSet[:-1])

    # Send txns and wait for some time
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    looper.runFor(5)

    # Last node is getting new prepares, but still can't order,
    # cause can't get quorum for prepare for previous batches
    assert len(behind_node.master_replica.prepares[(0, num_of_batches * 2)].voters) == 1
    assert len(behind_node.master_replica.prepares[(0, num_of_batches * 3)].voters) == 3
    assert behind_node.master_last_ordered_3PC[1] + num_of_batches * 2 == \
           master_node.master_last_ordered_3PC[1]

    # But last node requesting 3pc messages since ppSeqNo, so it can't order new txns
    assert behind_node.master_replica.lastPrePrepare.ppSeqNo == \
           master_node.master_last_ordered_3PC[1]


    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    looper.runFor(5)

    assert behind_node.master_last_ordered_3PC[1] == \
           master_node.master_last_ordered_3PC[1]
