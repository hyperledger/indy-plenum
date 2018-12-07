from plenum.common.constants import PREPREPARE, PREPARE, CHECKPOINT
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.delayers import ppDelay, pDelay, chk_delay
from plenum.test.helper import countDiscarded
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    checkNodeDataForInequality
from plenum.test.test_node import getNonPrimaryReplicas, TestReplica
from plenum.test.helper import sdk_send_random_and_check

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def discardCounts(replicas, pat):
    counts = {}
    for r in replicas:
        counts[r.name] = countDiscarded(r, pat)
    return counts


def test_non_primary_recvs_3phase_message_outside_watermarks(
        chkFreqPatched, reqs_for_logsize, looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    A node is slow in receiving PRE-PREPAREs and PREPAREs. A lot of requests
    are sent and the slow node has started receiving COMMITs outside of its
    watermarks and so stashes them. Also this node is slow in receiving
    CHECKPOINTs. So a catch-up does not occur on it.

    Then the slow node eventually receives the sent PRE-PREPAREs and PREPAREs
    and so orders the 3PC-batches between its watermarks. The other nodes
    discard the COMMITs from the slow node since they have already achieved
    stable checkpoints for these COMMITs.

    After that the slow node eventually receives the sent CHECKPOINTs from
    the other nodes and so stabilizes own completed checkpoints and updates its
    watermarks. A catch-up is not triggered because no received checkpoints are
    stashed. Since now the watermarks have been updated, the slow node
    processes 3PC-messages stashed earlier and its ledger becomes equal to the
    ledgers of the other nodes.
    """
    backupInstId = 1
    npr = getNonPrimaryReplicas(txnPoolNodeSet, backupInstId)

    slowReplica = npr[0]
    slowNode = slowReplica.node

    slowNode.nodeIbStasher.delay(ppDelay(300, backupInstId))
    slowNode.nodeIbStasher.delay(pDelay(300, backupInstId))
    slowNode.nodeIbStasher.delay(chk_delay(300))

    initialDomainLedgerSize = slowNode.domainLedger.size
    oldStashCount = slowReplica.spylog.count(TestReplica.stashOutsideWatermarks.__name__)

    # 1. Send requests more than fit between the watermarks on the slow node
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, reqs_for_logsize + 2)

    # Verify that the slow node stashes the batches outside of its watermarks
    newStashCount = slowReplica.spylog.count(TestReplica.stashOutsideWatermarks.__name__)
    assert newStashCount > oldStashCount

    oldDiscardCounts = discardCounts([n.replicas[backupInstId] for n in txnPoolNodeSet if n != slowNode],
                                     'achieved stable checkpoint')

    # 2. Deliver the sent PREPREPAREs and PREPAREs to the slow node
    slowNode.nodeIbStasher.reset_delays_and_process_delayeds(PREPREPARE, PREPARE)

    # Verify that the slow node orders the 3PC-batches between its watermarks
    # but no more.
    looper.runFor(waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)))

    checkNodeDataForInequality(slowNode, *[n for n in txnPoolNodeSet if n != slowNode])
    assert slowNode.domainLedger.size - initialDomainLedgerSize == reqs_for_logsize

    # Also verify that the other nodes discard the COMMITs from the slow node
    # since they have already achieved stable checkpoints for these COMMITs.
    counts = discardCounts(
        [n.replicas[backupInstId] for n in txnPoolNodeSet if n != slowNode],
        'achieved stable checkpoint')
    for nm, count in counts.items():
        assert count > oldDiscardCounts[nm]

    oldCatchupTimes = slowNode.spylog.count(Node.start_catchup)

    # 3. Deliver the sent CHECKPOINTs to the slow node
    slowNode.nodeIbStasher.reset_delays_and_process_delayeds(CHECKPOINT)

    # Verify that the slow node processes 3PC-messages stashed earlier and its
    # ledger becomes equal to the ledgers of the other nodes while a catch-up
    # is not made.
    waitNodeDataEquality(looper, slowNode, *[n for n in txnPoolNodeSet if n != slowNode])
    assert slowNode.domainLedger.size - initialDomainLedgerSize == reqs_for_logsize + 2
    newCatchupTimes = slowNode.spylog.count(Node.start_catchup)
    assert newCatchupTimes == oldCatchupTimes
