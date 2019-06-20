from plenum.common.constants import ROOT_HASH, MULTI_SIGNATURE, PROOF_NODES, TXN_TYPE, DATA, TXN_TIME, STATE_PROOF, \
    MULTI_SIGNATURE_VALUE, MULTI_SIGNATURE_PARTICIPANTS, MULTI_SIGNATURE_SIGNATURE, \
    MULTI_SIGNATURE_VALUE_LEDGER_ID, \
    MULTI_SIGNATURE_VALUE_STATE_ROOT, MULTI_SIGNATURE_VALUE_TXN_ROOT, MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT, \
    MULTI_SIGNATURE_VALUE_TIMESTAMP, DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.common.plenum_protocol_version import PlenumProtocolVersion
from plenum.common.request import SafeRequest
from plenum.common.txn_util import get_type, get_from, get_req_id, get_seq_no, get_txn_time
from plenum.common.types import f
from plenum.common.util import get_utc_epoch
from plenum.test.bls.helper import validate_multi_signature, validate_proof_for_write, validate_proof_for_read
from plenum.test.buy_handler import BuyHandler
from plenum.test.constants import GET_BUY
from plenum.test.helper import wait_for_requests_ordered, \
    randomOperation, sdk_send_random_requests, sdk_json_couples_to_request_list, sdk_send_random_and_check, \
    sdk_json_to_request_object

nodeCount = 4
nodes_wth_bls = 4


def check_result(txnPoolNodeSet, req, should_have_proof):
    for node in txnPoolNodeSet:
        req_handler = node.read_manager.request_handlers[GET_BUY]
        key = BuyHandler.prepare_buy_key(req.identifier, req.reqId)
        _, _, _, proof = req_handler.lookup(key, with_proof=True)

        txn_time = get_utc_epoch()
        result = req_handler.make_result(req,
                                         {TXN_TYPE: "buy"},
                                         2,
                                         txn_time,
                                         proof)
        assert result
        assert result[DATA] == {TXN_TYPE: "buy"}
        assert result[f.IDENTIFIER.nm] == req.identifier
        assert result[f.REQ_ID.nm] == req.reqId
        assert result[f.SEQ_NO.nm] == 2
        assert result[TXN_TIME] == txn_time

        if should_have_proof:
            assert result[STATE_PROOF] == proof
            assert validate_proof_for_read(result, req)
        else:
            assert STATE_PROOF not in result


def test_make_proof_bls_enabled(looper, txnPoolNodeSet,
                                sdk_pool_handle, sdk_wallet_client):
    reqs = sdk_json_couples_to_request_list(
        sdk_send_random_requests(
            looper, sdk_pool_handle, sdk_wallet_client, 1))
    wait_for_requests_ordered(looper, txnPoolNodeSet, reqs)

    req = reqs[0]
    for node in txnPoolNodeSet:
        req_handler = node.read_manager.request_handlers[GET_BUY]
        key = BuyHandler.prepare_buy_key(req.identifier, req.reqId)
        _, _, _, proof = req_handler.lookup(key, with_proof=True)
        assert proof
        assert ROOT_HASH in proof
        assert MULTI_SIGNATURE in proof
        assert PROOF_NODES in proof

        multi_sig = proof[MULTI_SIGNATURE]
        assert MULTI_SIGNATURE_SIGNATURE in multi_sig
        assert MULTI_SIGNATURE_PARTICIPANTS in multi_sig
        assert MULTI_SIGNATURE_VALUE in multi_sig

        multi_sig_value = multi_sig[MULTI_SIGNATURE_VALUE]
        assert MULTI_SIGNATURE_VALUE_LEDGER_ID in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_STATE_ROOT in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_TXN_ROOT in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT in multi_sig_value
        assert MULTI_SIGNATURE_VALUE_TIMESTAMP in multi_sig_value
        # check that multi sig values are in order
        value_keys = list(multi_sig_value.keys())
        assert [MULTI_SIGNATURE_VALUE_LEDGER_ID,
                MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT,
                MULTI_SIGNATURE_VALUE_STATE_ROOT,
                MULTI_SIGNATURE_VALUE_TIMESTAMP,
                MULTI_SIGNATURE_VALUE_TXN_ROOT] == value_keys

        assert validate_multi_signature(proof, txnPoolNodeSet)


def test_make_result_bls_enabled(looper, txnPoolNodeSet,
                                 sdk_pool_handle, sdk_wallet_client):
    req_dict, _ = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)[0]
    req = sdk_json_to_request_object(req_dict)
    wait_for_requests_ordered(looper, txnPoolNodeSet, [req])

    assert req.protocolVersion
    assert req.protocolVersion >= PlenumProtocolVersion.STATE_PROOF_SUPPORT.value
    check_result(txnPoolNodeSet, req, True)


def test_make_result_no_protocol_version(looper, txnPoolNodeSet):
    request = SafeRequest(identifier="1" * 16,
                          reqId=1,
                          operation=randomOperation(),
                          signature="signature",
                          protocolVersion=CURRENT_PROTOCOL_VERSION)
    request.protocolVersion = None
    check_result(txnPoolNodeSet, request, False)


def test_make_result_protocol_version_less_than_state_proof(looper,
                                                            txnPoolNodeSet):
    request = SafeRequest(identifier="1" * 16,
                          reqId=1,
                          operation=randomOperation(),
                          signature="signature",
                          protocolVersion=CURRENT_PROTOCOL_VERSION)
    request.protocolVersion = 0
    check_result(txnPoolNodeSet, request, False)


def test_proof_in_write_reply(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client):
    resp = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                     sdk_pool_handle, sdk_wallet_client, 1)

    req = resp[0][0]
    result = resp[0][1]['result']

    assert result
    assert get_type(result) == "buy"
    assert get_from(result) == req[f.IDENTIFIER.nm]
    assert get_req_id(result) == req[f.REQ_ID.nm]
    assert get_seq_no(result)
    assert get_txn_time(result)
    assert STATE_PROOF in result

    state_proof = result[STATE_PROOF]
    assert ROOT_HASH in state_proof
    assert MULTI_SIGNATURE in state_proof
    assert PROOF_NODES in state_proof

    multi_sig = state_proof[MULTI_SIGNATURE]
    assert MULTI_SIGNATURE_SIGNATURE in multi_sig
    assert MULTI_SIGNATURE_PARTICIPANTS in multi_sig
    assert MULTI_SIGNATURE_VALUE in multi_sig

    multi_sig_value = multi_sig[MULTI_SIGNATURE_VALUE]
    assert MULTI_SIGNATURE_VALUE_LEDGER_ID in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_STATE_ROOT in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_TXN_ROOT in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT in multi_sig_value
    assert MULTI_SIGNATURE_VALUE_TIMESTAMP in multi_sig_value

    assert validate_multi_signature(state_proof, txnPoolNodeSet)
    assert validate_proof_for_write(result)


def test_make_proof_committed_head_used(looper, txnPoolNodeSet,
                                        sdk_pool_handle, sdk_wallet_client):
    req_dict, _ = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)[0]
    req = sdk_json_to_request_object(req_dict)
    wait_for_requests_ordered(looper, txnPoolNodeSet, [req])
    key = BuyHandler.prepare_buy_key(req.identifier, req.reqId)

    for node in txnPoolNodeSet:
        node.states[DOMAIN_LEDGER_ID].set(key, b'somevalue')

    check_result(txnPoolNodeSet, req, True)
