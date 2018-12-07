import types

import pytest

from plenum.common.messages.node_messages import CatchupReq
from plenum.test import waits
from plenum.test.helper import sdk_send_random_requests, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.test_node import checkNodesConnected, getNonPrimaryReplicas
from stp_core.common.log import getlogger

# Do not remove the next import

logger = getlogger()

TestRunningTimeLimitSec = 180


@pytest.fixture(scope="module")
def reduced_catchup_timeout_conf(tconf, request):
    defaultCatchupTransactionsTimeout = tconf.CatchupTransactionsTimeout
    tconf.CatchupTransactionsTimeout = 10

    def reset():
        tconf.CatchupTransactionsTimeout = defaultCatchupTransactionsTimeout

    request.addfinalizer(reset)
    return tconf


def testNodeRequestingTxns(reduced_catchup_timeout_conf, txnPoolNodeSet,
                           looper, tdir, tconf,
                           allPluginsPath, sdk_pool_handle, sdk_wallet_steward, sdk_wallet_client):
    """
    A newly joined node is catching up and sends catchup requests to other
    nodes but one of the nodes does not reply and the newly joined node cannot
    complete the process till the timeout and then requests the missing
    transactions.
    """

    def ignoreCatchupReq(self, req, frm):
        logger.info("{} being malicious and ignoring catchup request {} "
                    "from {}".format(self, req, frm))

    # One of the node does not process catchup request.
    npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    badReplica = npr[0]
    badNode = badReplica.node
    badNode.nodeMsgRouter.routes[CatchupReq] = types.MethodType(
        ignoreCatchupReq, badNode.ledgerManager)
    more_requests = 10
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, more_requests)

    _, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        'EpsilonSteward', 'Epsilon', tdir, tconf,
        allPluginsPath=allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Since one of the nodes does not reply, this new node will experience a
    # timeout and retry catchup requests, hence a long test timeout.
    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet)) + \
              reduced_catchup_timeout_conf.CatchupTransactionsTimeout
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                         customTimeout=timeout)

    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 2)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                         customTimeout=timeout)
