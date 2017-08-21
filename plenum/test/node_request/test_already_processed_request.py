from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    send_signed_requests, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.spy_helpers import getAllReturnVals


def test_already_processed_requests(looper, txnPoolNodeSet, client1,
                                    wallet1, client1Connected):
    """
    Client re-sending request and checking that nodes picked the reply from
    ledger and did not process the request again
    """

    def get_method_call_count(method):
        counts = set()
        for node in txnPoolNodeSet:
            c = node.spylog.count(method)
            counts.add(c)
        assert len(counts) == 1
        return counts.pop()

    def get_getReplyFromLedger_call_count():
        return get_method_call_count(
            next(iter(txnPoolNodeSet)).getReplyFromLedger)

    def get_recordAndPropagate_call_count():
        return get_method_call_count(
            next(iter(txnPoolNodeSet)).recordAndPropagate)

    def get_last_returned_val():
        rvs = []
        for node in txnPoolNodeSet:
            rv = getAllReturnVals(node, node.getReplyFromLedger)
            rvs.append(rv[0])
        # All items are same in the list
        assert rvs.count(rvs[0]) == len(txnPoolNodeSet)
        return rvs[0]

    # Send a request
    rlc1 = get_getReplyFromLedger_call_count()
    rpc1 = get_recordAndPropagate_call_count()
    req1, = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    rlc2 = get_getReplyFromLedger_call_count()
    rpc2 = get_recordAndPropagate_call_count()
    assert rlc2 - rlc1 == 1     # getReplyFromLedger was called
    assert rpc2 - rpc1 == 1     # recordAndPropagate was called
    r1 = get_last_returned_val()
    assert r1 is None       # getReplyFromLedger returned None since had not seen request

    req2, = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    assert req2.reqId != req1.reqId
    rlc3 = get_getReplyFromLedger_call_count()
    rpc3 = get_recordAndPropagate_call_count()
    assert rlc3 - rlc2 == 1     # getReplyFromLedger was called again
    assert rpc3 - rpc2 == 1     # recordAndPropagate was called again
    r2 = get_last_returned_val()
    assert r2 is None       # getReplyFromLedger returned None since had not seen request

    # Reply for the first request, which is going to be sent again
    rep1 = client1.getReply(req1.identifier, req1.reqId)

    # Clear the client so that test waits for client getting reply
    client1.inBox.clear()
    client1.txnLog.reset()

    # Client re-sending request
    req3, = send_signed_requests(client1, [req1, ])
    waitForSufficientRepliesForRequests(looper, client1, requests=[req3, ])
    assert req3.reqId == req1.reqId
    rlc4 = get_getReplyFromLedger_call_count()
    rpc4 = get_recordAndPropagate_call_count()
    assert rlc4 - rlc3 == 1     # getReplyFromLedger was called again
    assert rpc4 - rpc3 == 0     # recordAndPropagate was not called
    r3 = get_last_returned_val()
    # getReplyFromLedger did not return None this time since had seen request
    assert r3 is not None
    rep3 = client1.getReply(req3.identifier, req3.reqId)

    # Since txnTime is not stored in ledger and reading from ledger return
    # all possible fields from transactions
    rep3 = {k: v for k, v in rep3[0].items() if v is not None}
    rep1 = {k: v for k, v in rep1[0].items() if k in rep3}

    assert rep3 == rep1     # The reply client got is same as the previous one
