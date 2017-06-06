from time import perf_counter

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.util import updateNamedTuple
from plenum.test.delayers import cqDelay, cr_delay
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

    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    new_node_replica_names = {r.instId: r.name for r in new_node.replicas}

    for node in txnPoolNodeSet[:-1]:
        for replica in node.replicas:
            for commit in replica.commits.values():
                for v in commit.voters:
                    assert new_node_replica_names[replica.instId] not in v
            for prepare in replica.prepares.values():
                for v in prepare.voters:
                    assert new_node_replica_names[replica.instId] not in v

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])

    looper.runFor(20)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)

    from itertools import chain

    for node in txnPoolNodeSet[:-1]:
        for replica in node.replicas:
            c = chain.from_iterable([votes.voters for votes in replica.commits.values()])
            assert len([x for x in c if new_node.name in x]) == 1, \
                "{} not found in commit voters".format(new_node.name)
            c = chain.from_iterable([votes.voters for votes in replica.prepares.values()])
            assert len([x for x in c if new_node.name in x]) == 1, \
                "{} not found in prepare voters".format(new_node.name)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])
