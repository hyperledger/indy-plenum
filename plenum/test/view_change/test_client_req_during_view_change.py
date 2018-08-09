import pytest

from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_and_check_replies, sdk_gen_request, \
    checkDiscardMsg


def test_client_msg_discard_in_view_change_integration(txnPoolNodeSet,
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
                                            sdk_wallet_client, 4)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, discard_reqs)
        assert "Client request is discarded since view " \
               "change is in progress" in e.args[0]


def test_client_msg_discard_in_view_change_unit(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    node.view_changer.view_change_in_progress = True
    msg = sdk_gen_request("op").as_dict
    node.unpackClientMsg(msg, "frm")
    checkDiscardMsg([node, ], msg, "view change in progress")
