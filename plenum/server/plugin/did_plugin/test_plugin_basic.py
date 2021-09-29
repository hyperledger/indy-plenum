import pytest

from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_gen_request, \
    sdk_sign_and_submit_req_obj, sdk_get_reply, sdk_sign_request_objects, sdk_send_signed_requests, \
    sdk_get_and_check_replies
from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.util import randomString
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID, dummy_field_length
from plenum.test.plugin.demo_plugin.constants import GET_BAL


def test_plugin_setup(txn_pool_node_set_post_creation):
    """
    Test that plugin's ledger and state are setup
    """
    for node in txn_pool_node_set_post_creation:
        assert AUCTION_LEDGER_ID in node.ledger_ids
        assert AUCTION_LEDGER_ID in node.ledgerManager.ledgerRegistry
        assert node.ledger_ids == node.ledgerManager.ledger_sync_order
        assert AUCTION_LEDGER_ID in node.states


def test_plugin_client_req_fields(txn_pool_node_set_post_creation, looper,
                                  sdk_wallet_steward, sdk_pool_handle):
    """
    Test that plugin's addition of request fields and their validation is
    successful
    """
    op = {
        TXN_TYPE: GET_BAL,
        DATA: {'id': '123'}
    }

    # Valid field value results in successful processing
    req_obj = sdk_gen_request(op, identifier=sdk_wallet_steward[1],
                              fix_length_dummy=randomString(dummy_field_length))
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_steward,
                                      req_obj)
    sdk_get_reply(looper, req)

    # Invalid field value results in proper failure
    _, did = sdk_wallet_steward
    req = sdk_gen_request(op, identifier=did, fix_length_dummy=randomString(dummy_field_length + 1))
    reqs = sdk_sign_request_objects(looper, sdk_wallet_steward, [req])
    reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)

    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, reqs)
    assert 'should have length' in e._excinfo[1].args[0]
