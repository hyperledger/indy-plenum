import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.startable import Mode
from plenum.test.delayers import cqDelay
from plenum.test.helper import sendRandomRequests
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.test_node import checkNodesConnected
from plenum.test import waits


logger = getlogger()

txnCount = 10


@pytest.fixture("module")
def nodeStashingOrderedRequests(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    looper, newNode, client, wallet, _, _ = nodeCreatedAfterSomeTxns
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cqDelay(5))
    txnPoolNodeSet.append(newNode)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet[:-1])
    sendRandomRequests(wallet, client, 10)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    def stashing():
        assert newNode.mode != Mode.participating
        assert len(newNode.stashedOrderedReqs) > 0
        # assert len(newNode.reqsFromCatchupReplies) > 0

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(eventually(stashing, retryWait=1, timeout=timeout))


@pytest.mark.skip(reason="SOV-552. Incomplete")
def testNodeNotProcessingOrderedReqsWhileCatchingUp(
        nodeStashingOrderedRequests):
    """
    Check that node does not execute requests while catching up
    :return:
    """


@pytest.mark.skip(reason="SOV-553. Incomplete")
def testExecutedInOrderAfterCatchingUp(txnPoolNodeSet,
                                       nodeStashingOrderedRequests):
    """
    After catching up, while executing check for already see client id and
    request id., maintain a list of seen client id and request ids, the node
    while catching up keeps track of seen client ids and request id
    Reset monitor after executing all stashed requests so no view change can
    be proposed
    :return:
    """
    newNode = txnPoolNodeSet[-1]
