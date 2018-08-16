import pytest

from plenum.common.exceptions import RequestNackedException, \
    InvalidClientRequest
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
                                            sdk_wallet_client, 1)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, discard_reqs)
        assert "Client request is discarded since view " \
               "change is in progress" in e.args[0]


def test_client_msg_discard_in_view_change_with_dict(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    node.view_changer.view_change_in_progress = True
    msg = sdk_gen_request("op").as_dict
    with pytest.raises(InvalidClientRequest) as e:
        node.unpackClientMsg(msg, "frm")
        assert "Client request is discarded since view " \
               "change is in progress" in e.args[0]
    checkDiscardMsg([node, ], msg, "view change in progress")


def test_client_msg_discard_in_view_change_with_request(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    node.view_changer.view_change_in_progress = True
    msg = sdk_gen_request("op")
    with pytest.raises(InvalidClientRequest) as e:
        node.unpackClientMsg(msg, "frm")
        assert "Client request is discarded since view " \
               "change is in progress" in e.args[0]
    checkDiscardMsg([node, ], msg.as_dict, "view change in progress")
