import json

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, TXN_TYPE, DATA
from plenum.common.request import Request
from plenum.common.util import randomString

from plenum.test.helper import sdk_random_request_objects, sdk_multisign_request_object, sdk_get_and_check_replies, \
    sdk_send_signed_requests, sdk_gen_request, sdk_sign_and_submit_req_obj, sdk_get_reply
from plenum.test.plugin.demo_plugin import dummy_field_length
from plenum.test.plugin.demo_plugin.constants import PLACE_BID, AMOUNT, AUCTION_START


def test_plugin_client_req_fields(txn_pool_node_set_post_creation, looper,
                                  sdk_wallet_steward, sdk_pool_handle):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'xyz'}
    }

    # Valid field value results in successful processing
    req_obj = sdk_gen_request(op, identifier=sdk_wallet_steward[1],
                              fix_length_dummy=randomString(dummy_field_length))
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet_steward,
                                      req_obj)
    sdk_get_and_check_replies(looper, [req])

    req = Request(**req[0])
    value = txn_pool_node_set_post_creation[0].seqNoDB.get_by_full_digest(req.digest)
    assert value == req.payload_digest

    ledger_id, seq_no = txn_pool_node_set_post_creation[0].seqNoDB.get_by_full_digest(req.payload_digest)
    assert ledger_id is not None and seq_no is not None
