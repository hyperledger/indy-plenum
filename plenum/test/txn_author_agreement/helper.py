import base64
import json
from _sha256 import sha256

import base58
from indy.ledger import build_txn_author_agreement_request, build_get_txn_author_agreement_request, \
    build_get_acceptance_mechanisms_request, build_disable_all_txn_author_agreements_request

from typing import NamedTuple, Dict, Optional
from plenum.common.constants import CONFIG_LEDGER_ID, STATE_PROOF, ROOT_HASH, PROOF_NODES, MULTI_SIGNATURE, \
    MULTI_SIGNATURE_PARTICIPANTS, MULTI_SIGNATURE_SIGNATURE, MULTI_SIGNATURE_VALUE, MULTI_SIGNATURE_VALUE_LEDGER_ID, \
    MULTI_SIGNATURE_VALUE_STATE_ROOT, MULTI_SIGNATURE_VALUE_TXN_ROOT, MULTI_SIGNATURE_VALUE_POOL_STATE_ROOT, \
    MULTI_SIGNATURE_VALUE_TIMESTAMP, TXN_AUTHOR_AGREEMENT_TEXT, TXN_AUTHOR_AGREEMENT_VERSION, \
    AML_VERSION, AML, AML_CONTEXT, GET_TXN_AUTHOR_AGREEMENT_DIGEST, GET_TXN_AUTHOR_AGREEMENT_VERSION, \
    OP_FIELD_NAME, DATA, TXN_TIME, REPLY, \
    TXN_METADATA, TXN_METADATA_SEQ_NO, TXN_METADATA_TIME, GET_TXN_AUTHOR_AGREEMENT_AML_VERSION, \
    GET_TXN_AUTHOR_AGREEMENT_AML_TIMESTAMP, TXN_AUTHOR_AGREEMENT_AML, TXN_AUTHOR_AGREEMENT_RETIREMENT_TS, TXN_TYPE, \
    TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_DIGEST, TXN_AUTHOR_AGREEMENT_RATIFICATION_TS, TXN_AUTHOR_AGREEMENT_DISABLE
from plenum.common.types import f
from plenum.common.util import randomString
from plenum.server.request_handlers.static_taa_helper import StaticTAAHelper
from plenum.server.request_handlers.txn_author_agreement_aml_handler import TxnAuthorAgreementAmlHandler
from plenum.server.request_managers.write_request_manager import WriteRequestManager
from plenum.test.helper import sdk_sign_and_submit_req, sdk_get_and_check_replies, sdk_sign_and_submit_op
from state.pruning_state import PruningState

TaaData = NamedTuple("TaaData", [
    ("text", str),
    ("version", str),
    ("seq_no", int),
    ("txn_time", int),
    ("digest", str)
])

TaaAmlData = NamedTuple("TaaAmlData", [
    ("version", str),
    ("aml", dict),
    ("amlContext", str),
    ("seq_no", int),
    ("txn_time", int)
])


def sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet, version: str,
                                  text: Optional[str] = None,
                                  ratified: Optional[int] = None,
                                  retired: Optional[int] = None):
    req = looper.loop.run_until_complete(build_txn_author_agreement_request(sdk_wallet[1], text, version,
                                                                            ratified, retired))
    rep = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet, req)
    return sdk_get_and_check_replies(looper, [rep])[0]


def sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet):
    req = looper.loop.run_until_complete(build_disable_all_txn_author_agreements_request(sdk_wallet[1]))
    rep = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet, req)
    return sdk_get_and_check_replies(looper, [rep])[0]


def set_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet, text: str, version: str, ratified: int, retired: Optional[int]
) -> TaaData:
    reply = sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet, version, text,
                                          ratified=ratified, retired=retired)[1]

    assert reply[OP_FIELD_NAME] == REPLY
    result = reply[f.RESULT.nm]

    return TaaData(
        text, version,
        seq_no=result[TXN_METADATA][TXN_METADATA_SEQ_NO],
        txn_time=result[TXN_METADATA][TXN_METADATA_TIME],
        # TODO: Add ratified?
        digest=StaticTAAHelper.taa_digest(text, version)
    )


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


def sdk_get_taa_aml(looper, sdk_pool_handle, sdk_wallet,
                    version: Optional[str] = None,
                    timestamp: Optional[int] = None):
    req = looper.loop.run_until_complete(build_get_acceptance_mechanisms_request(sdk_wallet[1], timestamp, version))
    rep = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet, req)
    return sdk_get_and_check_replies(looper, [rep])[0]


def get_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet,
        digest: Optional[str] = None,
        version: Optional[str] = None,
        timestamp: Optional[int] = None
) -> TaaData:
    reply = sdk_get_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet,
        digest=digest, version=version, timestamp=timestamp
    )[1]

    assert reply[OP_FIELD_NAME] == REPLY
    result = reply[f.RESULT.nm]

    return None if result[DATA] is None else TaaData(
        text=result[DATA][TXN_AUTHOR_AGREEMENT_TEXT],
        version=result[DATA][TXN_AUTHOR_AGREEMENT_VERSION],
        seq_no=result[f.SEQ_NO.nm],
        txn_time=result[TXN_TIME],
        digest=result[DATA][TXN_AUTHOR_AGREEMENT_DIGEST]
    )


def get_aml_req_handler(node):
    aml_req_handler = node.write_manager.request_handlers[TXN_AUTHOR_AGREEMENT_AML][0]
    assert isinstance(aml_req_handler, TxnAuthorAgreementAmlHandler)
    return aml_req_handler


def taa_digest(text: str, version: str) -> str:
    return sha256('{}{}'.format(version, text).encode()).hexdigest()


def check_state_proof(result, expected_key: Optional = None, expected_value: Optional = None):
    # TODO: This was copy-pasted from indy node (and extended), probably there should be better place for it
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

    if expected_key is not None:
        proof_nodes = base64.b64decode(state_proof[PROOF_NODES])
        root_hash = base58.b58decode(state_proof[ROOT_HASH])
        assert PruningState.verify_state_proof(root_hash,
                                               expected_key,
                                               expected_value,
                                               proof_nodes, serialized=True)

    # TODO: Validate signatures as well?


def expected_state_data(data: TaaData) -> Dict:
    return {
        'lsn': data.seq_no,
        'lut': data.txn_time,
        'val': {
            TXN_AUTHOR_AGREEMENT_TEXT: data.text,
            TXN_AUTHOR_AGREEMENT_VERSION: data.version,
            TXN_AUTHOR_AGREEMENT_DIGEST: StaticTAAHelper.taa_digest(data.text, data.version),
            TXN_AUTHOR_AGREEMENT_RATIFICATION_TS: data.txn_time
        }
    }


def expected_data(data: TaaData):
    return {
        TXN_AUTHOR_AGREEMENT_TEXT: data.text,
        TXN_AUTHOR_AGREEMENT_VERSION: data.version,
        TXN_AUTHOR_AGREEMENT_DIGEST: StaticTAAHelper.taa_digest(data.text, data.version),
        TXN_AUTHOR_AGREEMENT_RATIFICATION_TS: data.txn_time
    }, data.seq_no, data.txn_time


def expected_aml_data(data: TaaAmlData):
    return {
               AML_VERSION: data.version,
               AML: data.aml,
               AML_CONTEXT: data.amlContext
           }, data.seq_no, data.txn_time


def gen_random_txn_author_agreement(text_size=1024, version_size=16):
    return randomString(text_size), randomString(version_size)


# TODO might make sense to use sdk's api
def calc_taa_digest(text, version):
    return WriteRequestManager._taa_digest(text, version)
