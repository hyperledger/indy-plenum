import pytest

from plenum.test.checkpoints.helper import checkRequestCounts
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually
from plenum.test.helper import sdk_send_batches_of_random_and_check

CHK_FREQ = 5


@pytest.fixture(scope='function',
                params=['greater_than_checkpoint', 'lesser_than_checkpoint', 'equal_to_checkpoint'])
def sent_batches(request, chkFreqPatched):
    # Test with number of sent batches greater than checkpoint,
    # lesser than checkpoint and equal to checkpont.
    if request.param == 'greater_than_checkpoint':
        return CHK_FREQ + 2
    if request.param == 'lesser_than_checkpoint':
        return CHK_FREQ - 2
    if request.param == 'equal_to_checkpoint':
        return CHK_FREQ


def test_checkpoint_across_views(sent_batches, chkFreqPatched, looper, txnPoolNodeSet,
                                 sdk_pool_handle, sdk_wallet_client):
    """
    Test checkpointing across views.
    This test checks that checkpointing and garbage collection works correctly
    no matter if view change happened before a checkpoint or after a checkpoint
    """
    batch_size = chkFreqPatched.Max3PCBatchSize
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         batch_size * sent_batches, sent_batches)

    # Check that correct garbage collection happens
    non_gced_batch_count = (sent_batches - CHK_FREQ) if sent_batches >= CHK_FREQ else sent_batches
    looper.run(eventually(checkRequestCounts, txnPoolNodeSet, batch_size * non_gced_batch_count,
                          non_gced_batch_count, retryWait=1))

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # Check that after view change, proper clean up is done
    for node in txnPoolNodeSet:
        for r in node.replicas.values():
            # Checkpoint was started after sending audit txn
            # assert not r.checkpoints
            # No stashed checkpoint for previous view
            assert not [view_no for view_no in r.stashedRecvdCheckpoints if view_no < r.viewNo]
            assert r._h == 0
            # from audit txn
            assert r._lastPrePrepareSeqNo == 1
            assert r.h == 0
            assert r.H == r._h + chkFreqPatched.LOG_SIZE

    # All this manipulations because after view change we will send an empty batch for auditing
    checkRequestCounts(txnPoolNodeSet, 0, 1)
    if sent_batches > CHK_FREQ:
        expected_batch_count = sent_batches - CHK_FREQ + 1
        additional_after_vc = 0
    elif sent_batches == CHK_FREQ:
        expected_batch_count = 0
        additional_after_vc = 0
        sent_batches = CHK_FREQ - 1
    else:
        expected_batch_count = sent_batches + 1
        additional_after_vc = 1

    # Even after view change, chekpointing works
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         batch_size * sent_batches, sent_batches)

    looper.run(eventually(checkRequestCounts, txnPoolNodeSet, batch_size * (expected_batch_count - additional_after_vc),
                          expected_batch_count, retryWait=1))

    # Send more batches so one more checkpoint happens. This is done so that
    # when this test finishes, all requests are garbage collected and the
    # next run of this test (with next param) has the calculations correct
    more = CHK_FREQ - expected_batch_count
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         batch_size * more, more)
    looper.run(eventually(checkRequestCounts, txnPoolNodeSet, 0, 0, retryWait=1))
