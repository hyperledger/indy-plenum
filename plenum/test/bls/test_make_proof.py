from base58 import b58encode
from plenum.test.helper import sdk_send_random_and_check
from plenum.common.types import f
from plenum.common.constants import ROOT_HASH


def test_make_proof(looper, sdk_wallet_steward, sdk_pool_handle, txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    req_handler = node.getDomainReqHandler()
    req1, _ = sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 1)[0]
    # Save headHash after first request
    head1 = req_handler.state.headHash
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 1)
    # Save headHash after second request
    head2 = req_handler.state.headHash
    # Build path to first request
    path1 = req_handler.prepare_buy_key(req1[f.IDENTIFIER.nm], req1[f.REQ_ID.nm])
    # Check that if parameter "head_hash" is None, then we make proof for commitedHeadHash (by default)
    assert b58encode(head2).decode("utf-8") == req_handler.make_proof(path1)[ROOT_HASH]
    # Check that if parameter "head_hash" is not None, then we make proof for given headHash
    assert b58encode(head1).decode("utf-8") == req_handler.make_proof(path1, head_hash=head1)[ROOT_HASH]
