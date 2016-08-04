from plenum.test.eventually import eventually
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality


def testNewNodeCatchup(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    """
    A new node that joins after some transactions should eventually get
    those transactions.
    TODO: Test correct statuses are exchanged
    TODO: Test correct consistency proofs are generated
    :return:
    """
    looper, newNode, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:4], retryWait=1, timeout=5))


def testNodeCatchupAfterRestart(txnPoolNodeSet,
                                nodeSetWithNodeAddedAfterSomeTxns):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """

    looper, newNode, _, client = nodeSetWithNodeAddedAfterSomeTxns
    newNode.stop()
    sendReqsToNodesAndVerifySuffReplies(looper, client, 5)
    newNode.start(looper.loop)
    looper.run(eventually(checkNodeLedgersForEquality, newNode,
                          *txnPoolNodeSet[:4], retryWait=1, timeout=5))


def testNodeDoesNotParticipateUntilCaughtUp(txnPoolNodeSet,
                                            nodeSetWithNodeAddedAfterSomeTxns):
    """
    A new node that joins after some transactions should stash new transactions
    until it has caught up
    :return:
    """
    looper, newNode, _, client = nodeSetWithNodeAddedAfterSomeTxns
    sendReqsToNodesAndVerifySuffReplies(looper, client, 5)

    for node in txnPoolNodeSet[:4]:
        for replica in node.replicas:
            for commit in replica.commits.values():
                assert newNode.name not in commit.voters
            for prepare in replica.prepares.values():
                assert newNode.name not in prepare.voters
