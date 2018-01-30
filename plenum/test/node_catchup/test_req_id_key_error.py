from plenum.common.types import f
from plenum.common.txn_util import reqToTxn
from plenum.test.helper import signed_random_requests
from stp_core.common.log import getlogger

logger = getlogger()


def test_req_id_key_error(testNode, wallet1):
    #create random transactions
    count_of_txn = 3
    reqs = signed_random_requests(wallet1, count_of_txn)
    txns = []
    #prepare transactions and remove reqId from
    for i, req in enumerate(reqs):
        txnreq = reqToTxn(req)
        txnreq[f.SEQ_NO.nm] = i
        txnreq.pop(f.REQ_ID.nm)
        txns.append(txnreq)
    node = testNode
    logger.debug(txns)
    node.updateSeqNoMap(txns)
