import pytest

from plenum.common.log import getlogger
from plenum.common.looper import Looper
from plenum.common.util import get_size
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet


logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


def testRequestsSize(looper, txnPoolNodeSet, poolTxnClientNames,
                     tdirWithPoolTxns, poolTxnData):
    """
    Client should not be using node registry but pool transaction file
    :return:
    """
    clients = []
    for name in poolTxnClientNames:
        seed = poolTxnData["seeds"][name].encode()
        client, wallet = buildPoolClientAndWallet((name, seed),
                                                  tdirWithPoolTxns)
        looper.add(client)
        ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                      *txnPoolNodeSet)
        clients.append((client, wallet))

    n = 250
    timeOutPerReq = 10
    for (client, wallet) in clients:
        logger.debug("{} sending {} requests".format(client, n))
        chunkSize = n // 5
        for i in range(5):
            sendReqsToNodesAndVerifySuffReplies(looper, wallet, client,
                                                chunkSize, 1, timeOutPerReq)
            logger.debug("{} sent {} requests".format(client, chunkSize))
        # sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, n, 1, 10)
        logger.debug("{} sent {} requests".format(client, n))
    for node in txnPoolNodeSet:
        logger.debug("{} has requests {} with size {}".
                     format(node, len(node.requests), get_size(node.requests)))
