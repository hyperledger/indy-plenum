import pytest

from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeLedgersEquality
from plenum.test.pool_transactions.helper import ensureNodeDisconnectedFromPool
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone, \
    TestNode
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5


@pytest.mark.skip(reason="SOV-939")
def testNewNodeCatchup(newNodeCaughtUp):
    """
    A new node that joins after some transactions should eventually get
    those transactions.
    TODO: Test correct statuses are exchanged
    TODO: Test correct consistency proofs are generated
    :return:
    """
    pass


@pytest.mark.skip(reason="SOV-939")
def testPoolLegerCatchupBeforeDomainLedgerCatchup(txnPoolNodeSet,
                                                  newNodeCaughtUp):
    """
    For new node, this should be the sequence of events:
     1. Pool ledger starts catching up.
     2. Pool ledger completes catching up.
     3. Domain ledger starts catching up
     4. Domain ledger completes catching up
    Every node's pool ledger starts catching up before it
    """
    newNode = newNodeCaughtUp
    starts = newNode.ledgerManager.spylog.getAll(
        TestLedgerManager.startCatchUpProcess.__name__)
    completes = newNode.ledgerManager.spylog.getAll(
        TestLedgerManager.catchupCompleted.__name__)
    startTimes = {}
    completionTimes = {}
    for start in starts:
        startTimes[start.params.get('ledgerType')] = start.endtime
    for comp in completes:
        completionTimes[comp.params.get('ledgerType')] = comp.endtime
    assert startTimes[0] < completionTimes[0] < \
           startTimes[1] < completionTimes[1]


@pytest.mark.skip(reason="SOV-554. "
                         "Test implementation pending, although bug fixed")
def testDelayedLedgerStatusNotChangingState():
    """
    Scenario: When a domain `LedgerStatus` arrives when the node is in
    `participating` mode, the mode should not change to `discovered` if found
    the arriving `LedgerStatus` to be ok.
    """
    pass


# TODO: This test passes but it is observed that PREPAREs are not received at
# newly added node. If the stop and start steps are omitted then PREPAREs are
# received. Conclusion is that due to node restart, RAET is losing messages
# but its weird since prepares and commits are received which are sent before
# and after prepares, respectively. Here is the pivotal link
# https://www.pivotaltracker.com/story/show/127897273
@pytest.mark.skip(reason='fails, SOV-928, SOV-939')
def testNodeCatchupAfterRestart(newNodeCaughtUp, txnPoolNodeSet,
                                nodeSetWithNodeAddedAfterSomeTxns,
                                tdirWithPoolTxns, tconf, allPluginsPath):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """
    looper, newNode, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    logger.debug("Stopping node {} with pool ledger size {}".
                 format(newNode, newNode.poolManager.txnSeqNo))
    ensureNodeDisconnectedFromPool(looper, txnPoolNodeSet, newNode)
    looper.removeProdable(newNode)
    # for n in txnPoolNodeSet[:4]:
    #     for r in n.nodestack.remotes.values():
    #         if r.name == newNode.name:
    #             r.removeStaleCorrespondents()
    # looper.run(eventually(checkNodeDisconnectedFrom, newNode.name,
    #                       txnPoolNodeSet[:4], retryWait=1, timeout=5))
    # TODO: Check if the node has really stopped processing requests?
    logger.debug("Sending requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    restartedNewNode = TestNode(newNode.name,
                                basedirpath=tdirWithPoolTxns,
                                config=tconf,
                                ha=newNode.nodestack.ha,
                                cliha=newNode.clientstack.ha,
                                pluginPaths=allPluginsPath)
    logger.debug("Starting the stopped node, {}".format(restartedNewNode))
    looper.add(restartedNewNode)
    looper.run(checkNodesConnected(txnPoolNodeSet[:4] + [restartedNewNode]))
    waitNodeLedgersEquality(looper, restartedNewNode, *txnPoolNodeSet[:4])
    restartedNewNode.stop()


@pytest.mark.skip(reason='fails, SOV-928, SOV-939')
def testNodeDoesNotParticipateUntilCaughtUp(txnPoolNodeSet,
                                            nodeSetWithNodeAddedAfterSomeTxns):
    """
    A new node that joins after some transactions should stash new transactions
    until it has caught up
    :return:
    """
    looper, newNode, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)

    for node in txnPoolNodeSet[:4]:
        for replica in node.replicas:
            for commit in replica.commits.values():
                assert newNode.name not in commit.voters
            for prepare in replica.prepares.values():
                assert newNode.name not in prepare.voters
