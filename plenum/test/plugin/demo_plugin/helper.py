from plenum.common.constants import TXN_TYPE, DATA
from plenum.test.helper import sdk_gen_request, sdk_sign_and_submit_req_obj, sdk_get_reply
from plenum.test.plugin.demo_plugin.constants import AUCTION_START


def send_auction_txn(looper,
                     sdk_pool_handle, sdk_wallet_steward):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def successful_op(looper, op, sdk_wallet, sdk_pool_handle):
    req_obj = sdk_gen_request(op, identifier=sdk_wallet[1])
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle,
                                      sdk_wallet, req_obj)
    sdk_get_reply(looper, req)
