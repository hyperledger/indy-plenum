import pytest

from plenum.common.constants import NODE, TXN_TYPE, GET_TXN
from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_and_check_replies, sdk_gen_request, \
    checkDiscardMsg
from plenum.test.pool_transactions.helper import sdk_build_get_txn_request, sdk_sign_and_send_prepared_request
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope='function')
def test_node(test_node):
    test_node.view_changer = FakeSomething(view_change_in_progress=True,
                                           view_no=1)
    return test_node


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


def test_client_write_request_discard_in_view_change_with_dict(test_node):
    test_node.send_nack_to_client = check_nack_msg

    msg = sdk_gen_request({TXN_TYPE: NODE}).as_dict
    test_node.unpackClientMsg(msg, "frm")
    checkDiscardMsg([test_node, ], msg, "view change in progress")


def test_client_get_request_not_discard_in_view_change_with_dict(test_node):
    sender = "frm"
    msg = sdk_gen_request({TXN_TYPE: GET_TXN}).as_dict

    def post_to_client_in_box(received_msg, received_frm):
        assert received_frm == sender
        assert received_msg == msg
    test_node.postToClientInBox = post_to_client_in_box

    def discard(received_msg, reason, logLevel):
        assert False, "Message {} was discard with '{}'".format(received_msg, reason)
    test_node.discard = discard

    test_node.unpackClientMsg(msg, sender)


def test_client_msg_discard_in_view_change_with_request(test_node):
    test_node.send_nack_to_client = check_nack_msg

    msg = sdk_gen_request({TXN_TYPE: NODE})
    test_node.unpackClientMsg(msg, "frm")
    checkDiscardMsg([test_node, ], msg.as_dict, "view change in progress")


def check_nack_msg(req_key, reason, to_client):
    assert "Client request is discarded since view " \
           "change is in progress" == reason
