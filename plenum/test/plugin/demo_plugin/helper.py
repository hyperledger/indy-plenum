from plenum.common.constants import TXN_TYPE, DATA
from plenum.test.helper import sdk_gen_request, sdk_sign_and_submit_req_obj, sdk_get_reply, sdk_get_and_check_replies
from plenum.test.plugin.demo_plugin.constants import AUCTION_START, GET_AUCTION


def send_auction_txn(looper,
                     sdk_pool_handle, sdk_wallet_steward):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }
    return successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def send_get_auction_txn(looper,
                     sdk_pool_handle, sdk_wallet_steward):
    op = {
        TXN_TYPE: GET_AUCTION,
        DATA: {'auction_id': 'id'}
    }
    return successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def successful_op(looper, op, sdk_wallet, sdk_pool_handle):
    req_obj = sdk_gen_request(op, identifier=sdk_wallet[1])
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle,
                                      sdk_wallet, req_obj)
    return sdk_get_and_check_replies(looper, [req])
