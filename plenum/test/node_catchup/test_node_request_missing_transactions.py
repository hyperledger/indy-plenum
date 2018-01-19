import types

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from stp_core.common.log import getlogger
from plenum.common.messages.node_messages import CatchupReq
from plenum.test.helper import sendRandomRequests
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import checkNodesConnected, getNonPrimaryReplicas
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist


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
                           nodeCreatedAfterSomeTxns):
    """
    A newly joined node is catching up and sends catchup requests to other
    nodes but one of the nodes does not reply and the newly joined node cannot
    complete the process till the timeout and then requests the missing
    transactions.
    """
    looper, newNode, client, wallet, _, _ = nodeCreatedAfterSomeTxns
    new_node_ledger = newNode.ledgerManager.ledgerRegistry[DOMAIN_LEDGER_ID]
    old_size = len(new_node_ledger.ledger)
    old_size_others = txnPoolNodeSet[0].ledgerManager.ledgerRegistry[DOMAIN_LEDGER_ID].ledger.size

    # So nodes wont tell the clients about the newly joined node so they
    # dont send any request to the newly joined node
    for node in txnPoolNodeSet:
        node.sendPoolInfoToClients = types.MethodType(lambda x, y: None, node)

    def ignoreCatchupReq(self, req, frm):
        logger.info("{} being malicious and ignoring catchup request {} "
                    "from {}".format(self, req, frm))

    # One of the node does not process catchup request.
    npr = getNonPrimaryReplicas(txnPoolNodeSet, 0)
    badReplica = npr[0]
    badNode = badReplica.node
    txnPoolNodeSet.append(newNode)

    badNode.nodeMsgRouter.routes[CatchupReq] = types.MethodType(
        ignoreCatchupReq, badNode.ledgerManager)
    more_requests = 10
    sendRandomRequests(wallet, client, more_requests)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Since one of the nodes does not reply, this new node will experience a
    # timeout and retry catchup requests, hence a long test timeout.
    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet)) + \
        reduced_catchup_timeout_conf.CatchupTransactionsTimeout
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1],
                         customTimeout=timeout)
    new_size = len(new_node_ledger.ledger)

    # The new node ledger might catchup some transactions from the batch of
    # `more_request` transactions
    assert old_size_others - \
        old_size <= new_node_ledger.num_txns_caught_up <= new_size - old_size
    sendRandomRequests(wallet, client, 2)
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1],
                         customTimeout=timeout)
