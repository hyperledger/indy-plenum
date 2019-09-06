from plenum.test.checkpoints.helper import check_for_nodes, check_stable_checkpoint, check_num_unstable_checkpoints
from stp_core.loop.eventually import eventually
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check

# TODO: Probably these tests needs to be reworked, especially deletion test


def test_checkpoint_created(chkFreqPatched, tconf, looper, txnPoolNodeSet, sdk_pool_handle,
                            sdk_wallet_client, reqs_for_checkpoint):
    """
    After requests less than `CHK_FREQ`, there should be one checkpoint
    on each replica. After `CHK_FREQ`, one checkpoint should become stable
    """
    # Send one batch less so checkpoint is not created
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              reqs_for_checkpoint - (chkFreqPatched.Max3PCBatchSize))
    # Deliberately waiting so as to verify that checkpoint is not created nor stabilized
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, 0))
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_num_unstable_checkpoints, 0))

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              chkFreqPatched.Max3PCBatchSize)

    next_checkpoint = tconf.CHK_FREQ
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, next_checkpoint,
                          retryWait=1, timeout=timeout))
    check_for_nodes(txnPoolNodeSet, check_num_unstable_checkpoints, 0)


def test_old_checkpoint_deleted(tconf, looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, reqs_for_checkpoint):
    """
    Send requests more than twice of `CHK_FREQ`, there should be one new stable
    checkpoint on each replica. The old stable checkpoint should be removed
    """
    next_checkpoint = txnPoolNodeSet[0].master_replica._consensus_data.stable_checkpoint + 2 * tconf.CHK_FREQ
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2 * reqs_for_checkpoint)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, next_checkpoint,
                          retryWait=1, timeout=timeout))
    check_for_nodes(txnPoolNodeSet, check_num_unstable_checkpoints, 0)
