import pytest

from plenum.common.log import getlogger
from plenum.common.looper import Looper
from plenum.common.perf_util import get_size
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

    n = 100
    timeOutPerReq = 5
    for (client, wallet) in clients:
        logger.debug("{} sending {} requests".format(client, n))
        sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, n, 1,
                                            timeOutPerReq)
        logger.debug("{} sent {} requests".format(client, n))
    for node in txnPoolNodeSet:
        total = 0
        s = get_size(node.requests)
        logger.debug("{} has requests {} with size {}".
                     format(node, len(node.requests), s))
        total += s
        for replica in node.replicas:
            s = get_size(replica.prepares)
            logger.debug("{} has prepares {} with size {}".
                         format(replica, len(replica.prepares),
                                s))
            total += s
            s = get_size(replica.commits)
            logger.debug("{} has commits {} with size {}".
                         format(replica, len(replica.commits),
                                s))
            total += s
            s = get_size(replica.ordered)
            logger.debug("{} has ordered {} with size {}".
                         format(replica, len(replica.ordered),
                                s))
            total += s
            s = get_size(replica.commits)
            logger.debug("{} has commits {} with size {}".
                         format(replica, len(replica.commits),
                                s))
            total += s
        logger.debug("Print total size of {}'s data: {} for {} requests".
                     format(node, total/1024.0, n))
