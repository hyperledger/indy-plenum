from plenum.common.constants import CURRENT_PROTOCOL_VERSION, TXN_TYPE, DATA
from plenum.test.helper import waitReqNackFromPoolWithReason, \
    waitForSufficientRepliesForRequests, send_signed_requests, sign_requests, \
    waitRejectFromPoolWithReason
from plenum.test.plugin.demo_plugin.constants import AMOUNT, PLACE_BID, \
    AUCTION_START, AUCTION_END, AUCTION_LEDGER_ID


def test_plugin_static_validation(txnPoolNodeSet, looper, stewardWallet,
                                  steward1, client1Connected):
    """
    Check plugin static validation fails and passes
    """
    op = {
        TXN_TYPE: AUCTION_START
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, steward1,
                                  'attribute is missing or not in proper format')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: 'should be a dict but giving a string'
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, steward1,
                                  'attribute is missing or not in proper format')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'abc', AMOUNT: -3}
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitReqNackFromPoolWithReason(looper, txnPoolNodeSet, steward1,
                                  'must be present and should be a number')

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'abc', AMOUNT: 20}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)


def test_plugin_dynamic_validation(txnPoolNodeSet, looper, stewardWallet,
                                   steward1, client1Connected):
    """
    Check plugin dynamic validation fails and passes
    """
    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'abcdef'}
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitRejectFromPoolWithReason(looper, txnPoolNodeSet, steward1,
                                 'unknown auction')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'xyz'}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)

    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'xyz'}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)


def test_plugin_request_handling(txnPoolNodeSet, looper, stewardWallet,
                                 steward1, client1Connected):
    """
    Check that plugin requests are applied and change plugin's internal state
    """
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'pqr'}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'pqr', AMOUNT: 20}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)

    for node in txnPoolNodeSet:
        auctions = node.get_req_handler(AUCTION_LEDGER_ID).auctions
        assert 'pqr' in auctions
        assert auctions['pqr'][stewardWallet.defaultId] == 20

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'pqr', AMOUNT: 40}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)

    for node in txnPoolNodeSet:
        auctions = node.get_req_handler(AUCTION_LEDGER_ID).auctions
        assert 'pqr' in auctions
        assert auctions['pqr'][stewardWallet.defaultId] == 40

    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'pqr'}
    }
    reqs = send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitForSufficientRepliesForRequests(looper, steward1, requests=reqs)
