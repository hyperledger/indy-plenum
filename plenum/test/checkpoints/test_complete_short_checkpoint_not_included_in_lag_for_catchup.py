from logging import getLogger

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.server.replica import Replica
from plenum.test import waits

from plenum.test.checkpoints.conftest import tconf, chkFreqPatched, \
    reqs_for_checkpoint
from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    checkNodeDataForInequality, waitNodeDataInequality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone

logger = getLogger()

TestRunningTimeLimitSec = 200


CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_complete_short_checkpoint_not_included_in_lag_for_catchup(
        looper, chkFreqPatched, reqs_for_checkpoint, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward, sdk_wallet_client,
        tdir, tconf, allPluginsPath):
    """
    Verifies that if the first stored own checkpoint has a not aligned lower
    bound (this means that it was started after a catch-up), is complete
    and there is a quorumed stashed checkpoint from other replicas with
    the same end then this stashed checkpoint is not included into the lag
    for a catch-up, i.e. in such a case the lag which makes the node perform
    catch-up is Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 2 quorumed stashed
    received checkpoints.
    """
    max_batch_size = chkFreqPatched.Max3PCBatchSize

    _, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        'EpsilonSteward', 'Epsilon', tdir, tconf,
        allPluginsPath=allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
    # Epsilon did not participate in ordering of the batch with EpsilonSteward
    # NYM transaction and the batch with Epsilon NODE transaction.
    # Epsilon got these transactions via catch-up.

    # To complete the first checkpoint send requests for 1 checkpoint minus
    # 2 3PC-batches (since there are already 2 3PC-batches in the first
    # checkpoint : with EpsilonSteward NYM transaction and with Epsilon NODE
    # transaction). This checkpoint has a not aligned lower bound
    # on the new node replicas so it will not be stabilized on them.
    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           reqs_for_checkpoint - 2 * max_batch_size)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])

    # The master replica of the new node stops to receive 3PC-messages
    new_node.master_replica.threePhaseRouter.extend(
        (
            (PrePrepare, lambda *x, **y: None),
            (Prepare, lambda *x, **y: None),
            (Commit, lambda *x, **y: None),
        )
    )

    completed_catchups_before_reqs = get_number_of_completed_catchups(new_node)

    # Send requests for the new node's master replica to reach
    # Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1 quorumed stashed
    # checkpoints from others
    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP *
                                           reqs_for_checkpoint)

    # Give time for the new node to catch up if it is going to do it
    looper.runFor(waits.expectedPoolConsistencyProof(len(txnPoolNodeSet)) +
                  waits.expectedPoolCatchupTime(len(txnPoolNodeSet)))

    waitNodeDataInequality(looper, new_node, *txnPoolNodeSet[:-1])

    # Verify that the new node has not caught up
    assert get_number_of_completed_catchups(new_node) == completed_catchups_before_reqs

    # Send more requests for the new node's master replica to reach
    # Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 2 quorumed stashed
    # checkpoints from others
    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           reqs_for_checkpoint)

    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])

    # Verify that the new node has caught up
    assert get_number_of_completed_catchups(new_node) > completed_catchups_before_reqs


def get_number_of_completed_catchups(node):
    cnt = 0
    for entry in node.ledgerManager.spylog.getAll(
            node.ledgerManager.catchupCompleted):
        if entry.params['ledgerId'] == DOMAIN_LEDGER_ID:
            cnt += 1
    return cnt
