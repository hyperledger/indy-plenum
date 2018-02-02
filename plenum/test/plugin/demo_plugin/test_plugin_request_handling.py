import pytest

from plenum.common.constants import TXN_TYPE, DATA
from plenum.test.helper import waitReqNackFromPoolWithReason, \
    send_signed_requests, sign_requests, \
    waitRejectFromPoolWithReason, sdk_gen_request, sdk_sign_and_submit_req_obj, \
    sdk_get_reply
from plenum.test.plugin.demo_plugin.constants import AMOUNT, PLACE_BID, \
    AUCTION_START, AUCTION_END, AUCTION_LEDGER_ID


def successful_op(looper, op, sdk_wallet, sdk_pool_handle):
    req_obj = sdk_gen_request(op, identifier=sdk_wallet[1])
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle,
                                      sdk_wallet, req_obj)
    sdk_get_reply(looper, req)


def test_plugin_static_validation(nodeSet, looper, stewardWallet,
                                  steward1, client1Connected,
                                  sdk_wallet_steward, sdk_pool_handle):
    """
    Check plugin static validation fails and passes
    """
    op = {
        TXN_TYPE: AUCTION_START
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitReqNackFromPoolWithReason(looper, nodeSet, steward1,
                                  'attribute is missing or not in proper format')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: 'should be a dict but giving a string'
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitReqNackFromPoolWithReason(looper, nodeSet, steward1,
                                  'attribute is missing or not in proper format')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }

    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'abc', AMOUNT: -3}
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitReqNackFromPoolWithReason(looper, nodeSet, steward1,
                                  'must be present and should be a number')

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'abc', AMOUNT: 20}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def test_plugin_dynamic_validation(nodeSet, looper, stewardWallet,
                                   steward1, client1Connected,
                                   sdk_wallet_steward, sdk_pool_handle):
    """
    Check plugin dynamic validation fails and passes
    """
    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'abcdef'}
    }
    send_signed_requests(steward1, sign_requests(stewardWallet, [op, ]))
    waitRejectFromPoolWithReason(looper, nodeSet, steward1,
                                 'unknown auction')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'xyz'}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)

    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'xyz'}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


@pytest.fixture(scope="module")
def some_requests(nodeSet, looper, stewardWallet,
                                 steward1, client1Connected,
                                 sdk_wallet_steward, sdk_pool_handle):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'pqr'}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'pqr', AMOUNT: 20}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)

    for node in nodeSet:
        auctions = node.get_req_handler(AUCTION_LEDGER_ID).auctions
        assert 'pqr' in auctions
        assert auctions['pqr'][stewardWallet.defaultId] == 20

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'pqr', AMOUNT: 40}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)

    for node in nodeSet:
        auctions = node.get_req_handler(AUCTION_LEDGER_ID).auctions
        assert 'pqr' in auctions
        assert auctions['pqr'][stewardWallet.defaultId] == 40

    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'pqr'}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def test_plugin_request_handling(some_requests):
    """
    Check that plugin requests are applied and change plugin's internal state
    """
    pass
