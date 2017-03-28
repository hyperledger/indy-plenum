import types

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.log import getlogger
from plenum.common.types import CatchupReq
from plenum.test.helper import sendRandomRequests
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.test_node import checkNodesConnected

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist


logger = getlogger()


def testNodeRequestingTxns(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    """
    A newly joined node is catching up and sends catchup requests to other
    nodes but one of the nodes does not reply and the newly joined node cannot
    complete the process till the timeout and then requests the missing
    transactions.
    """
    looper, newNode, client, wallet, _, _ = nodeCreatedAfterSomeTxns
    # So nodes wont tell the clients about the newly joined node so they
    # dont send any request to the newly joined node
    for node in txnPoolNodeSet:
        node.sendPoolInfoToClients = types.MethodType(lambda x, y: None, node)

    txnPoolNodeSet.append(newNode)

    def ignoreCatchupReq(self, req, frm):
        logger.info("{} being malicious and ignoring catchup request {} "
                    "from {}".format(self, req, frm))

    # One of the node does not process catchup request.
    txnPoolNodeSet[0].nodeMsgRouter.routes[CatchupReq] = types.MethodType(
        ignoreCatchupReq, txnPoolNodeSet[0].ledgerManager)
    sendRandomRequests(wallet, client, 10)
    looper.run(checkNodesConnected(txnPoolNodeSet, overrideTimeout=60))
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=90))
