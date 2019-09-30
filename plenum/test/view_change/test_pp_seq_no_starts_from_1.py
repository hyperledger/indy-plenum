import pytest
from plenum.test.helper import checkViewNoForNodes
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.helper import sdk_send_random_and_check


# make sure that we send each reqeust individually to count pp_seq_no
# determenistically
@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1

    def reset():
        tconf.Max3PCBatchSize = oldSize

    request.addfinalizer(reset)
    return tconf


def test_pp_seq_not_starts_from_0_in_new_view(tconf, txnPoolNodeSet, looper,
                                              sdk_pool_handle, sdk_wallet_client):
    # This test fails since last ordered pre-prepare sequence number is
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)

    def chk(count):
        for node in txnPoolNodeSet:
            assert node.master_replica.last_ordered_3pc[1] == count

    batches_count = 0
    chk(batches_count)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    batches_count += 5
    chk(batches_count)

    new_view_no = ensure_view_change(looper, txnPoolNodeSet)
    assert new_view_no > old_view_no
    batches_count += 1
    chk(batches_count)  # After view_change, master primary must initiate 3pc batch

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    batches_count += 1
    chk(batches_count)  # new request for new view => last ordered 3PC is (0,2)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    batches_count += 5
    chk(batches_count)
