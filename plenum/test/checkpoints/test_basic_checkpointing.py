from stp_core.loop.eventually import eventually
from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.helper import sdk_send_random_and_check


def test_checkpoint_created(chkFreqPatched, looper, txnPoolNodeSet, sdk_pool_handle,
                            sdk_wallet_client, reqs_for_checkpoint):
    """
    After requests less than `CHK_FREQ`, there should be one checkpoint
    on each replica. After `CHK_FREQ`, one checkpoint should become stable
    """
    # Send one batch less so checkpoint is not created
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              reqs_for_checkpoint - (chkFreqPatched.Max3PCBatchSize))
    # Deliberately waiting so as to verify that not more than 1 checkpoint is
    # created
    looper.runFor(2)
    chkChkpoints(txnPoolNodeSet, 1)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                              chkFreqPatched.Max3PCBatchSize)

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1, timeout=timeout))


def test_old_checkpoint_deleted(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, reqs_for_checkpoint):
    """
    Send requests more than twice of `CHK_FREQ`, there should be one new stable
    checkpoint on each replica. The old stable checkpoint should be removed
    """
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2 * reqs_for_checkpoint)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 2, 0, retryWait=1, timeout=timeout))
