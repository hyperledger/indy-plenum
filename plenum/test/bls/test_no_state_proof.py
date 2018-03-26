from plenum.common.constants import TXN_TYPE, STATE_PROOF, DOMAIN_LEDGER_ID
from plenum.common.util import get_utc_epoch
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_json_to_request_object

nodeCount = 4
nodes_wth_bls = 0


def test_make_proof_bls_disabled(looper, txnPoolNodeSet,
                                 sdk_pool_handle, sdk_wallet_client):
    reqs = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client,
                                     1)

    req = reqs[0][1]['result']
    for node in txnPoolNodeSet:
        req_handler = node.get_req_handler(DOMAIN_LEDGER_ID)
        key = req_handler.prepare_buy_key(req['identifier'], req['reqId'])
        proof = req_handler.make_proof(key)
        assert not proof


def test_make_result_bls_disabled(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client):
    reqs = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client,
                                     1)

    req = reqs[0][1]['result']
    for node in txnPoolNodeSet:
        req_handler = node.get_req_handler(DOMAIN_LEDGER_ID)
        key = req_handler.prepare_buy_key(req['identifier'], req['reqId'])
        proof = req_handler.make_proof(key)
        result = req_handler.make_result(sdk_json_to_request_object(reqs[0][0]),
                                         {TXN_TYPE: "buy"},
                                         2,
                                         get_utc_epoch(),
                                         proof)
        assert STATE_PROOF not in result
