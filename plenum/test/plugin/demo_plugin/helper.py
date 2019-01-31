from plenum.common.constants import TXN_TYPE, DATA
from plenum.test.plugin.demo_plugin.constants import AUCTION_START
from plenum.test.plugin.demo_plugin.test_plugin_request_handling import successful_op


def send_auction_txn(looper,
                     sdk_pool_handle, sdk_wallet_steward):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)
