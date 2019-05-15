import json
from _sha256 import sha256

from indy.ledger import build_txn_author_agreement_request, build_get_txn_author_agreement_request

from typing import NamedTuple, Dict, Optional
from plenum.common.constants import CONFIG_LEDGER_ID, STATE_PROOF, ROOT_HASH, PROOF_NODES, MULTI_SIGNATURE, \
    MULTI_SIGNATURE_PARTICIPANTS, MULTI_SIGNATURE_SIGNATURE, MULTI_SIGNATURE_VALUE, MULTI_SIGNATURE_VALUE_LEDGER_ID, \
    MULTI_SIGNATURE_VALUE_STATE_ROOT, MULTI_SIGNATURE_VALUE_TXN_ROOT, MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT, \
    MULTI_SIGNATURE_VALUE_TIMESTAMP, TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_DIGEST, GET_TXN_AUTHOR_AGREEMENT_VERSION
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.helper import sdk_sign_and_submit_req, sdk_get_and_check_replies


TaaData = NamedTuple("TaaData", [
    ("text", str),
    ("version", str),
    ("seq_no", int),
    ("txn_time", int)
])


def sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet, text: str, version: str):
    req = looper.loop.run_until_complete(build_txn_author_agreement_request(sdk_wallet[1], text, version))
    rep = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet, req)
    return sdk_get_and_check_replies(looper, [rep])[0]


def sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet,
                                 digest: Optional[str] = None,
                                 version: Optional[str] = None,
                                 timestamp: Optional[int] = None):
    params = {}
    if digest is not None:
        params[GET_TXN_AUTHOR_AGREEMENT_DIGEST] = digest
    if version is not None:
        params[GET_TXN_AUTHOR_AGREEMENT_VERSION] = version
    if timestamp is not None:
        params['timestamp'] = timestamp
    req = looper.loop.run_until_complete(build_get_txn_author_agreement_request(sdk_wallet[1], json.dumps(params)))
    rep = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet, req)
    return sdk_get_and_check_replies(looper, [rep])[0]


def get_config_req_handler(node):
    config_req_handler = node.get_req_handler(CONFIG_LEDGER_ID)
    assert isinstance(config_req_handler, ConfigReqHandler)
    return config_req_handler


def taa_digest(text: str, version: str) -> str:
    return sha256('{}{}'.format(version, text).encode()).hexdigest()


def check_valid_proof(result):
    # TODO: This is copy-pasted from indy node, probably there should be better place for it
    assert STATE_PROOF in result

    state_proof = result[STATE_PROOF]
    assert ROOT_HASH in state_proof
    assert state_proof[ROOT_HASH]
    assert PROOF_NODES in state_proof
    assert state_proof[PROOF_NODES]
    assert MULTI_SIGNATURE in state_proof

    multi_sig = state_proof[MULTI_SIGNATURE]
    assert multi_sig
    assert multi_sig[MULTI_SIGNATURE_PARTICIPANTS]
    assert multi_sig[MULTI_SIGNATURE_SIGNATURE]
    assert MULTI_SIGNATURE_VALUE in multi_sig

    multi_sig_value = multi_sig[MULTI_SIGNATURE_VALUE]
    assert MULTI_SIGNATURE_VALUE_LEDGER_ID in multi_sig_value
    assert multi_sig_value[MULTI_SIGNATURE_VALUE_LEDGER_ID]
    assert MULTI_SIGNATURE_VALUE_STATE_ROOT in multi_sig_value
    assert multi_sig_value[MULTI_SIGNATURE_VALUE_STATE_ROOT]
    assert MULTI_SIGNATURE_VALUE_TXN_ROOT in multi_sig_value
    assert multi_sig_value[MULTI_SIGNATURE_VALUE_TXN_ROOT]
    assert MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT in multi_sig_value
    assert multi_sig_value[MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT]
    assert MULTI_SIGNATURE_VALUE_TIMESTAMP in multi_sig_value
    assert multi_sig_value[MULTI_SIGNATURE_VALUE_TIMESTAMP]


def expected_state_data(data: TaaData) -> Dict:
    return {
        'lsn': data.seq_no,
        'lut': data.txn_time,
        'val': {
            TXN_AUTHOR_AGREEMENT_TEXT: data.text,
            TXN_AUTHOR_AGREEMENT_VERSION: data.version
        }
    }


def expected_data(data: TaaData) -> Dict:
    return {
        TXN_AUTHOR_AGREEMENT_TEXT: data.text,
        TXN_AUTHOR_AGREEMENT_VERSION: data.version
    }, data.seq_no, data.txn_time
