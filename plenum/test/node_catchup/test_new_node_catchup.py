from time import perf_counter

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.util import updateNamedTuple
from plenum.test.delayers import cqDelay, cr_delay
from stp_zmq.zstack import KITZStack

from stp_core.loop.eventually import eventually
from plenum.common.types import HA
from stp_core.common.log import getlogger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_ledger_state
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone, \
    TestNode
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5


def testNewNodeCatchup(newNodeCaughtUp):
    """
    A new node that joins after some transactions should eventually get
    those transactions.
    TODO: Test correct statuses are exchanged
    TODO: Test correct consistency proofs are generated
    :return:
    """
    pass


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
        startTimes[start.params.get('ledgerId')] = start.endtime
    for comp in completes:
        completionTimes[comp.params.get('ledgerId')] = comp.endtime
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
def testNodeCatchupAfterRestart(newNodeCaughtUp, txnPoolNodeSet, tconf,
                                nodeSetWithNodeAddedAfterSomeTxns,
                                tdirWithPoolTxns, allPluginsPath):
    """
    A node that restarts after some transactions should eventually get the
    transactions which happened while it was down
    :return:
    """
    looper, newNode, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    logger.debug("Stopping node {} with pool ledger size {}".
                 format(newNode, newNode.poolManager.txnSeqNo))
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, newNode)
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
    logger.debug("Starting the stopped node, {}".format(newNode))
    nodeHa, nodeCHa = HA(*newNode.nodestack.ha), HA(*newNode.clientstack.ha)
    newNode = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                       ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(newNode)
    txnPoolNodeSet[-1] = newNode

    # Delay catchup reply processing so LedgerState does not change
    delay_catchup_reply = 5
    newNode.nodeIbStasher.delay(cr_delay(delay_catchup_reply))
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Make sure ledger starts syncing (sufficient consistency proofs received)
    looper.run(eventually(check_ledger_state, newNode, DOMAIN_LEDGER_ID,
                          LedgerState.syncing, retryWait=.5, timeout=5))

    confused_node = txnPoolNodeSet[0]
    cp = newNode.ledgerManager.ledgerRegistry[DOMAIN_LEDGER_ID].catchUpTill
    start, end = cp.seqNoStart, cp.seqNoEnd
    cons_proof = confused_node.ledgerManager._buildConsistencyProof(
        DOMAIN_LEDGER_ID, start, end)

    bad_send_time = None

    def chk():
        nonlocal bad_send_time
        entries = newNode.ledgerManager.spylog.getAll(
            newNode.ledgerManager.canProcessConsistencyProof.__name__)
        for entry in entries:
            # `canProcessConsistencyProof` should return False after `syncing_time`
            if entry.result == False and entry.starttime > bad_send_time:
                return
        assert False

    def send_and_chk(ledger_state):
        nonlocal bad_send_time, cons_proof
        bad_send_time = perf_counter()
        confused_node.ledgerManager.sendTo(cons_proof, newNode.name)
        # Check that the ConsistencyProof messages rejected
        looper.run(eventually(chk, retryWait=.5, timeout=5))
        check_ledger_state(newNode, DOMAIN_LEDGER_ID, ledger_state)

    send_and_chk(LedgerState.syncing)

    # Not accurate timeout but a conservative one
    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet)) + \
              2*delay_catchup_reply
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:4],
                         customTimeout=timeout)

    send_and_chk(LedgerState.synced)
    # cons_proof = updateNamedTuple(cons_proof, seqNoEnd=cons_proof.seqNoStart,
    #                               seqNoStart=cons_proof.seqNoEnd)
    # send_and_chk(LedgerState.synced)


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
