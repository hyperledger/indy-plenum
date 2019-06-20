import pytest

from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.exceptions import CommonSdkIOException
from plenum.test.plugin.demo_plugin.helper import successful_op
from plenum.test.helper import sdk_send_signed_requests, \
    sdk_sign_request_strings, sdk_get_and_check_replies
from plenum.test.plugin.demo_plugin.constants import AMOUNT, PLACE_BID, \
    AUCTION_START, AUCTION_END, AUCTION_LEDGER_ID
from stp_core.loop.eventually import eventually

whitelist = ["Can't parse parsed_req or op from message", ]


def test_plugin_static_validation(txn_pool_node_set_post_creation, looper,
                                  sdk_wallet_steward, sdk_pool_handle):
    """
    Check plugin static validation fails and passes
    """
    op = {
        TXN_TYPE: AUCTION_START
    }
    reqs = sdk_sign_request_strings(looper, sdk_wallet_steward, [op, ])
    reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)
    with pytest.raises(CommonSdkIOException) as exc_info:
        sdk_get_and_check_replies(looper, reqs)
    exc_info.match('Got an error with code 113')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: 'should be a dict but giving a string'
    }
    reqs = sdk_sign_request_strings(looper, sdk_wallet_steward, [op, ])
    reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)
    with pytest.raises(CommonSdkIOException) as exc_info:
        sdk_get_and_check_replies(looper, reqs)
    exc_info.match('Got an error with code 113')

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }

    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'abc', AMOUNT: -3}
    }
    reqs = sdk_sign_request_strings(looper, sdk_wallet_steward, [op, ])
    reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)
    with pytest.raises(CommonSdkIOException) as exc_info:
        sdk_get_and_check_replies(looper, reqs)
    exc_info.match('Got an error with code 113')

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'abc', AMOUNT: 20}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def test_plugin_dynamic_validation(txn_pool_node_set_post_creation, looper,
                                   sdk_wallet_steward, sdk_pool_handle):
    """
    Check plugin dynamic validation fails and passes
    """
    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'abcdef'}
    }
    reqs = sdk_sign_request_strings(looper, sdk_wallet_steward, [op, ])
    reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)
    with pytest.raises(CommonSdkIOException) as exc_info:
        sdk_get_and_check_replies(looper, reqs)
    exc_info.match('Got an error with code 113')

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
def some_requests(txn_pool_node_set_post_creation, looper,
                  sdk_wallet_steward, sdk_pool_handle):
    def check_auctions_amount(auc, expected_amount):
        assert did in auc['pqr']
        assert auc['pqr'][did] == expected_amount

    old_bls_store_size = None
    for node in txn_pool_node_set_post_creation:
        if old_bls_store_size is None:
            old_bls_store_size = node.bls_bft.bls_store._kvs.size
        else:
            assert node.bls_bft.bls_store._kvs.size == old_bls_store_size

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

    _, did = sdk_wallet_steward
    for node in txn_pool_node_set_post_creation:
        print(node.name)
        auctions = node.write_manager.request_handlers[PLACE_BID][0].auctions
        assert 'pqr' in auctions
        looper.run(eventually(check_auctions_amount, auctions, 20))

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {'id': 'pqr', AMOUNT: 40}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)

    for node in txn_pool_node_set_post_creation:
        auctions = node.write_manager.request_handlers[PLACE_BID][0].auctions
        assert 'pqr' in auctions
        looper.run(eventually(check_auctions_amount, auctions, 40))

    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {'id': 'pqr'}
    }
    successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)
    for node in txn_pool_node_set_post_creation:
        # Not all batches might have BLS-sig but at least one of them will have
        assert node.bls_bft.bls_store._kvs.size > old_bls_store_size


def test_plugin_request_handling(some_requests):
    """
    Check that plugin requests are applied and change plugin's internal state
    """
    pass
