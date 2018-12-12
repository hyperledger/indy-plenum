import pytest

from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_and_check_replies
from plenum.test.pool_transactions.helper import sdk_build_get_txn_request, sdk_sign_and_send_prepared_request


def test_client_write_request_discard_in_view_change_integration(txnPoolNodeSet,
                                                                 looper,
                                                                 sdk_pool_handle,
                                                                 sdk_wallet_client):
    '''
    Check that client requests sent in view change will discard.
    '''
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)

    for node in txnPoolNodeSet:
        node.view_changer.view_change_in_progress = True
    discard_reqs = sdk_send_random_requests(looper, sdk_pool_handle,
                                            sdk_wallet_client, 1)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, discard_reqs)
        assert "Client request is discarded since view " \
               "change is in progress" in e.args[0]


def test_client_get_request_not_discard_in_view_change_integration(txnPoolNodeSet,
                                                                   looper,
                                                                   sdk_pool_handle,
                                                                   sdk_wallet_client):
    '''
    Check that client requests sent in view change will discard.
    '''
    for node in txnPoolNodeSet:
        node.view_changer.view_change_in_progress = True
    _, steward_did = sdk_wallet_client
    request = sdk_build_get_txn_request(looper, steward_did, 1)

    sdk_request = sdk_sign_and_send_prepared_request(looper,
                                                     sdk_wallet_client,
                                                     sdk_pool_handle,
                                                     request)
    sdk_get_and_check_replies(looper, [sdk_request])
