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
                                            sdk_wallet_client, 1)
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, discard_reqs)
        assert "Client request is discarded since view " \
               "change is in progress" in e.args[0]


def test_client_msg_discard_in_view_change_with_dict(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    node.view_changer.view_change_in_progress = True
    node.send_nack_to_client = check_nack_msg

    msg = sdk_gen_request("op").as_dict
    node.unpackClientMsg(msg, "frm")
    checkDiscardMsg([node, ], msg, "view change in progress")


def test_client_msg_discard_in_view_change_with_request(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    node.view_changer.view_change_in_progress = True
    node.send_nack_to_client = check_nack_msg

    msg = sdk_gen_request("op")
    node.unpackClientMsg(msg, "frm")
    checkDiscardMsg([node, ], msg.as_dict, "view change in progress")


def check_nack_msg(req_key, reason, to_client):
    assert "Client request is discarded since view " \
           "change is in progress" == reason
