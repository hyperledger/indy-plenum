import types

import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.types import CatchupReq
from plenum.test.helper import sendRandomRequests
from plenum.test.node_catchup.helper import waitNodeLedgersEquality
from plenum.test.test_node import checkNodesConnected, getNonPrimaryReplicas
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist


logger = getlogger()


# @pytest.mark.skip(reason='fails, https://evernym.atlassian.net/browse/SOV-928')
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
    sendRandomRequests(wallet, client, 10)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Since one of the nodes does not reply, this new node will experience a
    # timeout and retry catchup requests, hence a long test timeout.
    # Dont reduce it.
    waitNodeLedgersEquality(looper, newNode, *txnPoolNodeSet[:-1],
                            customTimeout=90)
