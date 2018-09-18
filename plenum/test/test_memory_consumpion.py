import pytest

from stp_core.common.log import getlogger
from plenum.common.perf_util import get_size
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet

logger = getlogger()


@pytest.mark.skip(reason="SOV-537. "
                         "Temporary disabling it to check if tests run "
                         "on build pipeline")
def testRequestsSize(txnPoolNodesLooper, txnPoolNodeSet, poolTxnClientNames,
                     tdirWithPoolTxns, poolTxnData, noRetryReq):
    """
    Client should not be using node registry but pool transaction file
    :return:
    """
    clients = []
    for name in poolTxnClientNames:
        seed = poolTxnData["seeds"][name].encode()
        client, wallet = buildPoolClientAndWallet((name, seed),
                                                  tdirWithPoolTxns)
        txnPoolNodesLooper.add(client)
        ensureClientConnectedToNodesAndPoolLedgerSame(
            txnPoolNodesLooper, client, *txnPoolNodeSet)
        clients.append((client, wallet))

    numRequests = 250
    fVal = 1
    for (client, wallet) in clients:
        logger.debug("{} sending {} requests".format(client, numRequests))
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_steward, numRequests)
        logger.debug("{} sent {} requests".format(client, numRequests))
    for node in txnPoolNodeSet:
        logger.debug("{} has requests {} with size {}".
                     format(node, len(node.requests), get_size(node.requests)))
        for replica in node.replicas.values():
            logger.debug("{} has prepares {} with size {}".
                         format(replica, len(replica.prepares),
                                get_size(replica.prepares)))
            logger.debug("{} has commits {} with size {}".
                         format(replica, len(replica.commits),
                                get_size(replica.commits)))
