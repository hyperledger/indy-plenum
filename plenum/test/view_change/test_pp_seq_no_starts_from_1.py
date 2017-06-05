from plenum.test.helper import checkViewNoForNodes, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper
from plenum.test.view_change.helper import ensure_view_change


def test_pp_seq_no_starts_from_1_in_new_view(txnPoolNodeSet, looper, wallet1,
                                             client1, client1Connected, tconf):
    # This test fails since last ordered pre-prepare sequence number is
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)

    def chk(count):
        for node in txnPoolNodeSet:
            for r in node.replicas:
                assert r.last_ordered_3pc[1] == count

    chk(0)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 5)
    chk(5)
    new_view_no = ensure_view_change(looper, txnPoolNodeSet)
    assert new_view_no > old_view_no
    chk(0)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    chk(2)
