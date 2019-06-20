import json

from plenum.common.constants import TXN_TYPE, STATE_PROOF, DOMAIN_LEDGER_ID
from plenum.common.util import get_utc_epoch
from plenum.test.buy_handler import BuyHandler
from plenum.test.constants import GET_BUY
from plenum.test.helper import sdk_json_to_request_object, sdk_signed_random_requests

nodeCount = 4
nodes_wth_bls = 0


def test_make_proof_bls_disabled(looper, txnPoolNodeSet,
                                 sdk_wallet_client):
    req = json.loads(
        sdk_signed_random_requests(looper, sdk_wallet_client, 1)[0])

    for node in txnPoolNodeSet:
        req_handler = node.read_manager.request_handlers[GET_BUY]
        key = BuyHandler.prepare_buy_key(req['identifier'], req['reqId'])
        _, _, _, proof = req_handler.lookup(key, with_proof=True)
        assert not proof


def test_make_result_bls_disabled(looper, txnPoolNodeSet,
                                  sdk_wallet_client):
    req = json.loads(
        sdk_signed_random_requests(looper, sdk_wallet_client, 1)[0])

    for node in txnPoolNodeSet:
        req_handler = node.read_manager.request_handlers[GET_BUY]
        key = BuyHandler.prepare_buy_key(req['identifier'], req['reqId'])
        _, _, _, proof = req_handler.lookup(key, with_proof=True)
        result = req_handler.make_result(sdk_json_to_request_object(req),
                                                {TXN_TYPE: "buy"},
                                                2,
                                                get_utc_epoch(),
                                                proof)
        assert STATE_PROOF not in result
