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


def test_pp_seq_no_starts_from_0_in_new_view(tconf, txnPoolNodeSet, looper,
                                             sdk_pool_handle, sdk_wallet_client):
    # This test fails since last ordered pre-prepare sequence number is
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)

    def chk(count):
        for node in txnPoolNodeSet:
            assert node.master_replica.last_ordered_3pc[1] == count

    chk(0)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    chk(5)

    new_view_no = ensure_view_change(looper, txnPoolNodeSet)
    assert new_view_no > old_view_no
    chk(5)  # no new requests yet, so last ordered 3PC is (0,5)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    chk(1)  # new request for new view => last ordered 3PC is (0,1)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    chk(6)
