from collections import Callable

from plenum.common.types import f
from plenum.common.constants import NodeHooks, TXN_METADATA

from plenum.test.helper import sdk_signed_random_requests, sdk_send_signed_requests, \
    sdk_get_and_check_replies
from plenum.test.pool_transactions.helper import sdk_build_get_txn_request, \
    sdk_sign_and_send_prepared_request

foo = 'foo'


def register_hook(txnPoolNodeSet, hook_id, hook: Callable):
    for node in txnPoolNodeSet:
        node.register_hook(hook_id, hook)


def test_hook_pre_send_reply(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    def hook_add_field(*args, **kwargs):
        kwargs['committed_txns'][0][foo] = foo

    register_hook(txnPoolNodeSet, NodeHooks.PRE_SEND_REPLY, hook_add_field)

    # Reply on request
    signed_reqs = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    reqs = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    reply = sdk_get_and_check_replies(looper, reqs)[0]
    assert foo in reply[1][f.RESULT.nm]
    assert reply[1][f.RESULT.nm][foo] == foo

    # Reply on repeated request
    reqs = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    reply = sdk_get_and_check_replies(looper, reqs)[0]
    assert foo in reply[1][f.RESULT.nm]
    assert reply[1][f.RESULT.nm][foo] == foo

    # Reply on get_txn request
    _, did = sdk_wallet_client
    request = sdk_build_get_txn_request(
        looper, did, reply[1][f.RESULT.nm][TXN_METADATA][f.SEQ_NO.nm])
    request_couple = sdk_sign_and_send_prepared_request(looper,
                                                        sdk_wallet_client,
                                                        sdk_pool_handle,
                                                        request)
    reply = sdk_get_and_check_replies(looper, [request_couple])[0]
    assert foo in reply[1][f.RESULT.nm]['data']
    assert reply[1][f.RESULT.nm]['data'][foo] == foo
