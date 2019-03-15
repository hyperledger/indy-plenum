from logging import getLogger

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import getMaxFailures
from plenum.server.replica import Replica

from plenum.test.checkpoints.conftest import tconf, chkFreqPatched, \
    reqs_for_checkpoint
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, assertExp
from plenum.test.node_catchup.helper import waitNodeDataInequality, waitNodeDataEquality, \
    repair_broken_node, get_number_of_completed_catchups

logger = getLogger()

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_node_catchup_after_checkpoints(
        looper,
        chkFreqPatched,
        reqs_for_checkpoint,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client,
        broken_node_and_others):
    """
    A node misses 3pc messages and checkpoints during some period but later it
    stashes some amount of checkpoints and decides to catchup.
    """
    view_no = txnPoolNodeSet[0].viewNo
    max_batch_size = chkFreqPatched.Max3PCBatchSize
    broken_node, other_nodes = broken_node_and_others

    logger.info("Step 1: The node misses quite a lot of requests")

    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           reqs_for_checkpoint + max_batch_size)

    waitNodeDataInequality(looper, broken_node, *other_nodes)

    logger.info(
        "Step 2: The node gets requests but cannot process them because of "
        "missed ones. But the nodes eventually stashes some amount checkpoints "
        "after that the node starts catch up")

    repaired_node = repair_broken_node(broken_node)
    completed_catchups_before = get_number_of_completed_catchups(broken_node)

    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) *
                                           reqs_for_checkpoint - max_batch_size)

    waitNodeDataEquality(looper, repaired_node, *other_nodes,
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    # Note that the repaired node might not fill the gap of missed 3PC-messages
    # by requesting them from other nodes because these 3PC-batches start from
    # an already stabilized checkpoint, so a part of these 3PC-messages are
    # already unavailable

    # Verify that a catch-up was done
    completed_catchups_after = get_number_of_completed_catchups(repaired_node)
    assert completed_catchups_after > completed_catchups_before

    logger.info("Step 3: Check if the node is able to process requests")

    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           reqs_for_checkpoint + max_batch_size)

    waitNodeDataEquality(looper, repaired_node, *other_nodes)
