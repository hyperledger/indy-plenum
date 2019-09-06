import pytest

from plenum.common.exceptions import RequestRejectedException
from plenum.test import waits
from plenum.test.checkpoints.helper import checkRequestCounts, check_for_nodes, check_stable_checkpoint
from plenum.test.pool_transactions.helper import sdk_add_new_nym
from stp_core.loop.eventually import eventually
from plenum.test.helper import sdk_send_random_and_check


CHK_FREQ = 5


def test_request_older_than_stable_checkpoint_removed(chkFreqPatched, looper, txnPoolNodeSet, sdk_pool_handle,
                                                      sdk_wallet_steward, reqs_for_checkpoint):
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    max_batch_size = chkFreqPatched.Max3PCBatchSize

    # Send some requests (insufficient for checkpoint),
    # wait replies and check that current checkpoint is not stable
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 2 * max_batch_size)
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, 0, retryWait=1, timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 2 * max_batch_size, 2)

    # From the steward send a request creating a user with None role
    sdk_wallet_user = sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_steward)
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, 0, retryWait=1, timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 2 * max_batch_size + 1, 3)

    # From the created user send a request creating another user.
    # Dynamic validation of this request must fail since a user with None role cannot create users.
    # However, the 3PC-batch with the sent request must be ordered.
    with pytest.raises(RequestRejectedException):
        sdk_add_new_nym(looper, sdk_pool_handle, sdk_wallet_user)
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, 0, retryWait=1, timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 2 * max_batch_size + 2, 4)

    # Send more requests to cause checkpoint stabilization
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, max_batch_size)
    # Check that checkpoint is stable now
    # and verify that requests for it were removed
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, 5, retryWait=1, timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 0, 0)

    # Send more requests to cause new checkpoint
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, reqs_for_checkpoint + 1)
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, 10, retryWait=1, timeout=timeout))
    checkRequestCounts(txnPoolNodeSet, 1, 1)
