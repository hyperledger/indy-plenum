import json

from plenum.common.types import f
from plenum.test.helper import sdk_signed_random_requests
from stp_core.common.log import getlogger

logger = getlogger()


def test_req_id_key_error(testNode, looper, sdk_wallet_client):
    # create random transactions
    count_of_txn = 3
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, count_of_txn)
    txns = []

    # prepare transactions and remove reqId from
    for i, req in enumerate(reqs):
        j_req = json.loads(req)
        j_req[f.SEQ_NO.nm] = i
        j_req.pop(f.REQ_ID.nm)
        txns.append(j_req)

    node = testNode
    logger.debug(txns)
    node.updateSeqNoMap(txns)
