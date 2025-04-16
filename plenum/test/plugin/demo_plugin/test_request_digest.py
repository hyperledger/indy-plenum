import copy
import json

import pytest

from plenum import PLUGIN_CLIENT_REQUEST_FIELDS
from plenum.common.constants import CURRENT_PROTOCOL_VERSION, TXN_TYPE, DATA
from plenum.common.exceptions import RequestNackedException
from plenum.common.request import Request
from plenum.common.util import randomString

from plenum.test.helper import vdr_random_request_objects, vdr_multisign_request_object, vdr_get_and_check_replies, \
    vdr_send_signed_requests, vdr_gen_request, vdr_sign_and_submit_req_obj, vdr_get_reply
from plenum.test.plugin.demo_plugin import dummy_field_length
from plenum.test.plugin.demo_plugin.constants import PLACE_BID, AMOUNT, AUCTION_START


@pytest.fixture(scope='function')
def two_requests(looper, vdr_wallet_steward):
    wh, did = vdr_wallet_steward

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'xyz'}
    }

    req1 = vdr_gen_request(op, protocol_version=CURRENT_PROTOCOL_VERSION,
                           identifier=did).as_dict
    field = list(PLUGIN_CLIENT_REQUEST_FIELDS.keys())[0]
    req1[field] = 'x' * 10

    req2 = copy.deepcopy(req1)
    req2[field] = 'z' * 10

    req1 = vdr_multisign_request_object(looper, vdr_wallet_steward, json.dumps(req1))
    req_obj1 = Request(**json.loads(req1))

    req2 = vdr_multisign_request_object(looper, vdr_wallet_steward, json.dumps(req2))
    req_obj2 = Request(**json.loads(req2))

    assert req_obj1.payload_digest == req_obj2.payload_digest
    assert req_obj1.digest != req_obj2.digest
    return req1, req2


def test_plugin_digest_match_to_written(txn_pool_node_set_post_creation, looper,
                                        vdr_wallet_steward, vdr_pool_handle):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'xyz'}
    }

    # Valid field value results in successful processing
    req_obj = vdr_gen_request(op, identifier=vdr_wallet_steward[1],
                              fix_length_dummy=randomString(dummy_field_length))
    req = vdr_sign_and_submit_req_obj(looper, vdr_pool_handle, vdr_wallet_steward,
                                      req_obj)
    vdr_get_and_check_replies(looper, [req])

    req = Request(**req[0])
    value = txn_pool_node_set_post_creation[0].seqNoDB.get_by_full_digest(req.digest)
    assert value == req.payload_digest

    ledger_id, seq_no = txn_pool_node_set_post_creation[0].seqNoDB.get_by_payload_digest(req.payload_digest)
    assert ledger_id is not None and seq_no is not None


def test_send_same_txn_with_different_plugins(
        looper, txn_pool_node_set_post_creation, vdr_pool_handle, two_requests):
    req1, req2 = two_requests

    rep1 = vdr_send_signed_requests(vdr_pool_handle, [req1], looper)
    vdr_get_and_check_replies(looper, rep1)

    rep2 = vdr_send_signed_requests(vdr_pool_handle, [req2], looper)
    with pytest.raises(RequestNackedException) as e:
        vdr_get_and_check_replies(looper, rep2)
    e.match('Same txn was already ordered with different signatures or pluggable fields')
