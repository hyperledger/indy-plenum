from collections import OrderedDict
import json

from ledger.genesis_txn.genesis_txn_file_util import create_genesis_txn_init_ledger
from plenum.common.constants import TXN_TIME, TXN_TYPE, TARGET_NYM, ROLE, \
    ALIAS, VERKEY, FORCE
from plenum.common.types import f, OPERATION
from plenum.common.request import Request
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


def reqToTxn(req: Request, cons_time=None):
    """
    Transform a client request such that it can be stored in the ledger.
    Also this is what will be returned to the client in the reply
    :param req:
    :param cons_time: UTC epoch at which consensus was reached
    :return:
    """
    # TODO: we should not reformat transaction this way
    # When refactor keep in mind thought about back compatibility

    # data = req.signingState
    # res = {
    #     f.IDENTIFIER.nm: req.identifier,
    #     f.REQ_ID.nm: req.reqId,
    #     f.SIG.nm: req.signature
    # }
    # res.update(data[OPERATION])
    # return res

    if isinstance(req, dict):
        if TXN_TYPE in req:
            return req
        data = req
    else:
        data = req.as_dict

    res = {
        f.IDENTIFIER.nm: data.get(f.IDENTIFIER.nm),
        f.REQ_ID.nm: data[f.REQ_ID.nm],
        f.SIG.nm: data.get(f.SIG.nm, None),
        f.SIGS.nm: data.get(f.SIGS.nm, None),
        TXN_TIME: cons_time or data.get(TXN_TIME)
    }
    res.update(data[OPERATION])
    return res


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


def sdk_reqToTxn(sdk_req, cons_time=None):
    """
    Transform a client request such that it can be stored in the ledger.
    Also this is what will be returned to the client in the reply

    :param sdk_req: sdk request in str or dict type
    :param cons_time: UTC epoch at which consensus was reached
    :return:
    """
    # TODO: we should not reformat transaction this way
    # When refactor keep in mind thought about back compatibility

    if isinstance(sdk_req, dict):
        data = sdk_req
    elif isinstance(sdk_req, str):
        data = json.loads(sdk_req)
    else:
        raise TypeError(
            "Expected dict or str as input, but got: {}".format(type(sdk_req)))

    res = {
        f.IDENTIFIER.nm: data[f.IDENTIFIER.nm],
        f.REQ_ID.nm: data[f.REQ_ID.nm],
        f.SIG.nm: data.get(f.SIG.nm, None),
        f.SIGS.nm: data.get(f.SIGS.nm, None),
        TXN_TIME: cons_time or data.get(TXN_TIME)
    }
    res.update(data[OPERATION])
    return res
