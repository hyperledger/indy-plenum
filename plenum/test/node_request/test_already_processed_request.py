from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_signed_requests, sdk_eval_timeout, \
    sdk_get_replies, sdk_check_reply, sdk_signed_random_requests
from plenum.test.spy_helpers import getAllReturnVals


def test_already_processed_requests(looper, txnPoolNodeSet,
                                    sdk_pool_handle, sdk_wallet_client):
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

    def get_getReplyFromLedgerForRequest_call_count():
        return get_method_call_count(
            next(iter(txnPoolNodeSet)).getReplyFromLedgerForRequest)

    def get_recordAndPropagate_call_count():
        return get_method_call_count(
            next(iter(txnPoolNodeSet)).recordAndPropagate)

    def get_last_returned_val():
        rvs = []
        for node in txnPoolNodeSet:
            rv = getAllReturnVals(node, node.getReplyFromLedgerForRequest)
            rvs.append(rv[0])
        # All items are same in the list
        assert rvs.count(rvs[0]) == len(txnPoolNodeSet)
        return rvs[0]

    rlc1 = get_getReplyFromLedgerForRequest_call_count()
    rpc1 = get_recordAndPropagate_call_count()

    # Request which will be send twice
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, 1)

    # Send, check and getting reply from first request
    sdk_reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)
    total_timeout = sdk_eval_timeout(len(sdk_reqs), len(txnPoolNodeSet))
    request1 = sdk_get_replies(looper, sdk_reqs, timeout=total_timeout)
    for req_res in request1:
        sdk_check_reply(req_res)
    first_req_id = request1[0][0]['reqId']

    rlc2 = get_getReplyFromLedgerForRequest_call_count()
    rpc2 = get_recordAndPropagate_call_count()
    assert rlc2 - rlc1 == 1  # getReplyFromLedgerForRequest was called
    assert rpc2 - rpc1 == 1  # recordAndPropagate was called
    r1 = get_last_returned_val()
    assert r1 is None  # getReplyFromLedgerForRequest returned None since had not seen request

    # Request which we will send only once
    request2 = sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    second_req_id = request2[0][0]['reqId']

    assert second_req_id != first_req_id
    rlc3 = get_getReplyFromLedgerForRequest_call_count()
    rpc3 = get_recordAndPropagate_call_count()
    assert rlc3 - rlc2 == 1  # getReplyFromLedgerForRequest was called again
    assert rpc3 - rpc2 == 1  # recordAndPropagate was called again
    r2 = get_last_returned_val()
    assert r2 is None  # getReplyFromLedgerForRequest returned None since had not seen request

    # Reply for the first request, which is going to be sent again
    rep1 = request1[0][1]['result']

    # Client re-sending first request
    request3 = sdk_send_signed_requests(sdk_pool_handle, reqs)
    total_timeout = sdk_eval_timeout(len(request3), len(txnPoolNodeSet))
    request3 = sdk_get_replies(looper, request3, timeout=total_timeout)
    third_req_id = request3[0][0]['reqId']

    assert third_req_id == first_req_id
    rlc4 = get_getReplyFromLedgerForRequest_call_count()
    rpc4 = get_recordAndPropagate_call_count()
    assert rlc4 - rlc3 == 1  # getReplyFromLedgerForRequest was called again
    assert rpc4 - rpc3 == 0  # recordAndPropagate was not called
    r3 = get_last_returned_val()
    # getReplyFromLedgerForRequest did not return None this time since had seen request
    assert r3 is not None
    rep3 = request3[0][1]['result']

    # Since txnTime is not stored in ledger and reading from ledger return
    # all possible fields from transactions
    rep3 = {k: v for k, v in rep3.items() if v is not None}
    rep1 = {k: v for k, v in rep1.items() if k in rep3}

    assert rep3 == rep1  # The reply client got is same as the previous one
