from itertools import chain
from time import perf_counter

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.util import updateNamedTuple
from plenum.test.delayers import cqDelay, cr_delay
from plenum.test.test_node import ensureElectionsDone
from stp_zmq.zstack import KITZStack

from stp_core.common.log import getlogger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_ledger_state

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5
catchup_delay = 20


@pytest.fixture(scope="module")
def nodes_slow_to_process_catchup_reqs(txnPoolNodeSet):
    """
    This will make the new node slow to complete the catchup and hence will
    not send any 3PC messages till catchup is complete
    """
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cqDelay(catchup_delay))


def testNodeDoesNotParticipateUntilCaughtUp(txnPoolNodeSet,
                                            nodes_slow_to_process_catchup_reqs,
                                            nodeCreatedAfterSomeTxns):
    """
    A new node that joins after some transactions should stash new transactions
    until it has caught up
    :return:
    """

    looper, new_node, client, wallet, newStewardClient, newStewardWallet = \
        nodeCreatedAfterSomeTxns
    txnPoolNodeSet.append(new_node)
    old_nodes = txnPoolNodeSet[:-1]
    ensureElectionsDone(looper, txnPoolNodeSet)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    new_node_replica_names = {r.instId: r.name for r in new_node.replicas}

    def chk_commits_prepares_recvd(count):
        counts = {}
        for node in old_nodes:
            for replica in node.replicas:
                if replica.instId not in counts:
                    counts[replica.instId] = 0
                nm = new_node_replica_names[replica.instId]
                for commit in replica.commits.values():
                    counts[replica.instId] += int(nm in commit.voters)
                for prepare in replica.prepares.values():
                    counts[replica.instId] += int(nm in prepare.voters)
        for c in counts.values():
            assert count == c

    chk_commits_prepares_recvd(0)

    for node in old_nodes:
        node.resetDelays()
        node.force_process_delayeds()

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])

    looper.runFor(20)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 2)

    # Commits and Prepares are received by all old nodes
    chk_commits_prepares_recvd(2 * (len(old_nodes)))
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])
