from logging import getLogger

from plenum.common.startable import Mode
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.test import waits

from plenum.test.checkpoints.conftest import tconf, chkFreqPatched, \
    reqs_for_checkpoint
from plenum.test.delayers import cr_delay
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, \
    assertExp
from plenum.test.node_catchup.helper import waitNodeDataInequality, \
    repair_broken_node, ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually

logger = getLogger()

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_catchup_not_triggered_if_another_in_progress(
        looper,
        chkFreqPatched,
        reqs_for_checkpoint,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client,
        broken_node_and_others):
    """
    A node misses 3pc messages and checkpoints during some period but later it
    stashes some amount of checkpoints and starts catchup. When the node is
    performing the catchup, it receives more checkpoints enough to start a new
    catchup but it does not start it because the first catchup is in progress.
    """
    max_batch_size = chkFreqPatched.Max3PCBatchSize
    broken_node, other_nodes = broken_node_and_others

    logger.info("Step 1: The node misses quite a lot of 3PC-messages and checkpoints")

    send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                           sdk_pool_handle,
                                           sdk_wallet_client,
                                           reqs_for_checkpoint + max_batch_size)

    waitNodeDataInequality(looper, broken_node, *other_nodes)

    logger.info(
        "Step 2: The node receives 3PC-messages but cannot process them because of "
        "missed ones. But the node eventually stashes some amount of checkpoints "
        "and after that starts catchup")

    repaired_node = repair_broken_node(broken_node)

    initial_do_start_catchup_times = repaired_node.spylog.count(Node._do_start_catchup)
    initial_all_ledgers_caught_up = repaired_node.spylog.count(Node.allLedgersCaughtUp)

    with delay_rules(repaired_node.nodeIbStasher, cr_delay()):
        send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) *
                                               reqs_for_checkpoint - max_batch_size)

        ensure_all_nodes_have_same_data(looper, other_nodes)

        looper.run(eventually(lambda: assertExp(repaired_node.mode == Mode.discovering),
                              timeout=waits.expectedPoolInterconnectionTime(len(txnPoolNodeSet)) +
                                      waits.expectedPoolConsistencyProof(len(txnPoolNodeSet))))

        assert repaired_node.spylog.count(Node._do_start_catchup) - initial_do_start_catchup_times == 1

        logger.info(
            "Step 3: While doing the catchup, the node receives new checkpoints "
            "enough to start a new catchup but the node does not start it because "
            "the former is in progress")

        process_checkpoint_times_before = repaired_node.master_replica.spylog.count(Replica.process_checkpoint)

        send_reqs_batches_and_get_suff_replies(looper, txnPoolNodeSet,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) *
                                               reqs_for_checkpoint)

        # Wait until the node receives the new checkpoints from all the other nodes
        looper.run(
            eventually(lambda: assertExp(repaired_node.master_replica.spylog.count(Replica.process_checkpoint) -
                                         process_checkpoint_times_before ==
                                         (Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1) *
                                         (len(txnPoolNodeSet) - 1)),
                       timeout=waits.expectedPoolInterconnectionTime(len(txnPoolNodeSet))))

        # New catchup is not started when another one is in progress
        assert repaired_node.spylog.count(Node._do_start_catchup) - initial_do_start_catchup_times == 1
        assert repaired_node.mode == Mode.discovering

    logger.info("Step 4: The node completes the catchup. The ledger has been "
                "updated to the level determined on its start")

    looper.run(eventually(lambda: assertExp(repaired_node.mode == Mode.participating),
                          timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))))
    assert repaired_node.spylog.count(Node._do_start_catchup) - initial_do_start_catchup_times == 1
    assert repaired_node.spylog.count(Node.allLedgersCaughtUp) - initial_all_ledgers_caught_up == 1
    assert repaired_node.domainLedger.size == other_nodes[0].domainLedger.size
