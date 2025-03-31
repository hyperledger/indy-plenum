import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.test.helper import vdr_send_random_and_check, \
    vdr_send_random_requests, vdr_get_and_check_replies
from plenum.test.pool_transactions.helper import vdr_build_get_txn_request, vdr_sign_and_send_prepared_request


def test_client_write_request_discard_in_view_change_integration(txnPoolNodeSet,
                                                                 looper,
                                                                 vdr_pool_handle,
                                                                 vdr_wallet_client):
    '''
    Check that client requests sent in view change will discard.
    '''
    vdr_send_random_and_check(looper, txnPoolNodeSet, vdr_pool_handle,
                              vdr_wallet_client, 4)

    for node in txnPoolNodeSet:
        node.master_replica._consensus_data.waiting_for_new_view = True
    discard_reqs = vdr_send_random_requests(looper, vdr_pool_handle,
                                            vdr_wallet_client, 1)
    with pytest.raises(PoolLedgerTimeoutException) as e:
        vdr_get_and_check_replies(looper, discard_reqs)


def test_client_get_request_not_discard_in_view_change_integration(txnPoolNodeSet,
                                                                   looper,
                                                                   vdr_pool_handle,
                                                                   vdr_wallet_client):
    '''
    Check that client requests sent in view change will discard.
    '''
    for node in txnPoolNodeSet:
        node.master_replica._consensus_data.waiting_for_new_view = True
    _, steward_did = vdr_wallet_client
    request = vdr_build_get_txn_request(looper, steward_did, 1)

    sdk_request = vdr_sign_and_send_prepared_request(looper,
                                                     vdr_wallet_client,
                                                     vdr_pool_handle,
                                                     request)
    vdr_get_and_check_replies(looper, [sdk_request])
