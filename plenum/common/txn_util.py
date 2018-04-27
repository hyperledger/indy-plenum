import json
from collections import OrderedDict
from copy import deepcopy

from ledger.genesis_txn.genesis_txn_file_util import create_genesis_txn_init_ledger
from plenum.common.constants import TXN_TIME, TXN_TYPE, TARGET_NYM, ROLE, \
    ALIAS, VERKEY, FORCE, TXN_PAYLOAD, TXN_PAYLOAD_METADATA, TXN_SIGNATURE, TXN_METADATA, TXN_SIGNATURE_TYPE, ED25515, \
    TXN_SIGNATURE_FROM, TXN_SIGNATURE_VALUE, TXN_SIGNATURE_VALUES, TXN_PAYLOAD_DATA, TXN_PAYLOAD_METADATA_REQ_ID, \
    TXN_PAYLOAD_METADATA_FROM, TXN_PAYLOAD_PROTOCOL_VERSION, TXN_PAYLOAD_TYPE, TXN_METADATA_SEQ_NO, TXN_METADATA_TIME
from plenum.common.request import Request
from plenum.common.types import f, OPERATION
from stp_core.common.log import getlogger

logger = getlogger()


def getTxnOrderedFields():
    return OrderedDict([
        (f.IDENTIFIER.nm, (str, str)),
        (f.REQ_ID.nm, (str, int)),
        (f.SIG.nm, (str, str)),
        (TXN_TIME, (str, int)),
        (TXN_TYPE, (str, str)),
        (TARGET_NYM, (str, str)),
        (VERKEY, (str, str)),
        (ROLE, (str, str)),
        (ALIAS, (str, str)),
        (f.SIGS.nm, (str, str)),
    ])


def createGenesisTxnFile(genesisTxns, targetDir, fileName, fieldOrdering,
                         reset=True):
    ledger = create_genesis_txn_init_ledger(targetDir, fileName)

    if reset:
        ledger.reset()

    reqIds = {}
    for txn in genesisTxns:
        identifier = txn.get(f.IDENTIFIER.nm, "")
        if identifier not in reqIds:
            reqIds[identifier] = 0
        reqIds[identifier] += 1
        txn.update({
            f.REQ_ID.nm: reqIds[identifier],
            f.IDENTIFIER.nm: identifier
        })
        ledger.add(txn)
    ledger.stop()


def txnToReq(txn):
    """
    Transforms transactions to request form (not to Request)
    """
    txn = txn.copy()
    request = {}
    for field_name in [f.IDENTIFIER.nm, f.REQ_ID.nm, f.SIG.nm]:
        request[field_name] = txn.pop(field_name, None)
    request[OPERATION] = txn
    return request


def isTxnForced(txn):
    force = txn.get(FORCE)
    return str(force) == 'True'


def idr_from_req_data(data):
    if data.get(f.IDENTIFIER.nm):
        return data[f.IDENTIFIER.nm]
    else:
        return Request.gen_idr_from_sigs(data.get(f.SIGS.nm, {}))


def reqToTxn(req, txn_time=None):
    """
    Transform a client request such that it can be stored in the ledger.
    Also this is what will be returned to the client in the reply
    :param req:
    :return:
    """

    if isinstance(req, dict):
        req_data = req
    elif isinstance(req, str):
        req_data = json.loads(req)
    elif isinstance(req, Request):
        req_data = req.as_dict
    else:
        raise TypeError(
            "Expected dict or str as input, but got: {}".format(type(req)))

    result = {}
    result[TXN_PAYLOAD] = {}
    result[TXN_METADATA] = {}
    result[TXN_SIGNATURE] = {}

    # 1. Fill txm metadata (txnTime)
    # more metadata will be added by the Ledger
    if txn_time:
        append_txn_metadata(result, txn_time=txn_time)

    # 2. Fill Signature
    result[TXN_SIGNATURE][TXN_SIGNATURE_TYPE] = ED25515
    signatures = {req_data.get(f.IDENTIFIER.nm, None): req_data.get(f.SIG.nm, None)} \
        if req_data.get(f.SIG.nm, None) is not None \
        else req_data.get(f.SIGS.nm, {})
    result[TXN_SIGNATURE][TXN_SIGNATURE_VALUES] = [
        {
            TXN_SIGNATURE_FROM: frm,
            TXN_SIGNATURE_VALUE: sign,
        }
        for frm, sign in signatures.items()
    ]

    # 3. Fill Payload
    req_op = req_data[OPERATION]
    txn_payload = {}
    txn_payload[TXN_PAYLOAD_TYPE] = req_op.pop(TXN_TYPE, None)
    txn_payload[TXN_PAYLOAD_PROTOCOL_VERSION] = req_data.get(f.PROTOCOL_VERSION.nm, None)
    txn_payload[TXN_PAYLOAD_METADATA] = {}
    txn_payload[TXN_PAYLOAD_METADATA][TXN_PAYLOAD_METADATA_FROM] = req_data.get(f.IDENTIFIER.nm, None)
    txn_payload[TXN_PAYLOAD_METADATA][TXN_PAYLOAD_METADATA_REQ_ID] = req_data.get(f.REQ_ID.nm, None)
    txn_payload[TXN_PAYLOAD_DATA] = req_op
    result[TXN_PAYLOAD] = txn_payload

    return result


def append_txn_metadata(txn, seq_no=None, txn_time=None):
    if seq_no:
        txn[TXN_METADATA][TXN_METADATA_SEQ_NO] = seq_no
    if txn_time:
        txn[TXN_METADATA][TXN_METADATA_TIME] = txn_time
    return txn


def has_seq_no(txn):
    return txn[TXN_PAYLOAD_METADATA].get(TXN_METADATA_SEQ_NO, None) is not None


def transform_to_new_format(txn, seq_no):
    t = deepcopy(txn)

    result = {}
    result[TXN_PAYLOAD] = {}
    result[TXN_METADATA] = {}
    result[TXN_SIGNATURE] = {}

    result[TXN_METADATA][TXN_METADATA_TIME] = t.pop(TXN_TIME, None)
    result[TXN_METADATA][TXN_METADATA_SEQ_NO] = seq_no

    result[TXN_SIGNATURE][TXN_SIGNATURE_TYPE] = ED25515
    signatures = {t.get(f.IDENTIFIER.nm, None): t.get(f.SIG.nm, None)} if t.get(f.SIG.nm, None) is not None \
        else t.get(f.SIGS.nm, {})
    t.pop(f.SIG.nm, None)
    t.pop(f.SIGS.nm, None)

    result[TXN_SIGNATURE][TXN_SIGNATURE_VALUES] = [
        {
            TXN_SIGNATURE_FROM: frm,
            TXN_SIGNATURE_VALUE: sign,
        }
        for frm, sign in signatures.items()
    ]

    txn_payload = {}
    txn_payload[TXN_PAYLOAD_TYPE] = t.pop(TXN_TYPE)
    txn_payload[TXN_PAYLOAD_PROTOCOL_VERSION] = t.pop(f.PROTOCOL_VERSION.nm, None)
    txn_payload[TXN_PAYLOAD_METADATA] = {}
    txn_payload[TXN_PAYLOAD_METADATA][TXN_PAYLOAD_METADATA_FROM] = t.pop(f.IDENTIFIER.nm, None)
    txn_payload[TXN_PAYLOAD_METADATA][TXN_PAYLOAD_METADATA_REQ_ID] = t.pop(f.REQ_ID.nm, None)
    txn_payload[TXN_PAYLOAD_DATA] = t
    result[TXN_PAYLOAD] = txn_payload

    return result
