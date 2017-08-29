import pytest
from plenum.test.helper import checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.view_change.helper import ensure_view_change


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


def test_pp_seq_no_starts_from_0_in_new_view(
        tconf,
        txnPoolNodeSet,
        looper,
        wallet1,
        client1,
        client1Connected):
    # This test fails since last ordered pre-prepare sequence number is
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)

    def chk(count):
        for node in txnPoolNodeSet:
            assert node.master_replica.last_ordered_3pc[1] == count

    chk(0)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    chk(5)

    new_view_no = ensure_view_change(looper, txnPoolNodeSet)
    assert new_view_no > old_view_no
    chk(5)  # no new requests yet, so last ordered 3PC is (0,5)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    chk(1)  # new request for new view => last ordered 3PC is (0,1)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    chk(6)
