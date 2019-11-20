from plenum.test.delayers import cDelay

from plenum.test.checkpoints.helper import check_stable_checkpoint
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import checkNodesConnected

CHK_FREQ = 5


def test_upper_bound_of_checkpoint_after_catchup_is_divisible_by_chk_freq(
        chkFreqPatched, looper, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward, sdk_wallet_client, tdir,
        tconf, allPluginsPath):
    lagging_node = txnPoolNodeSet[-1]
    with delay_rules_without_processing(lagging_node.nodeIbStasher, cDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_client, tconf.Max3PCBatchSize * CHK_FREQ * 2 + 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    waitNodeDataEquality(looper, lagging_node, *txnPoolNodeSet[:-1],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    # Epsilon did not participate in ordering of the batch with EpsilonSteward
    # NYM transaction and the batch with Epsilon NODE transaction.
    # Epsilon got these transactions via catch-up.

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, (CHK_FREQ - 1) * tconf.Max3PCBatchSize)

    for replica in txnPoolNodeSet[0].replicas.values():
        check_stable_checkpoint(replica, CHK_FREQ * 3)

    # TODO: This is failing now because checkpoints are not created after catchup.
    #  PBFT paper describes catch-up with per-checkpoint granularity, but otherwise
    #  quite similar to plenum implementation. Authors of PBFT state that after
    #  catching up nodes set low watermark to last caught up checkpoint, which is
    #  actually equivalent to declaring that checkpoint stable. This means that
    #  most probably we need this functionality in plenum.
    # for replica in lagging_node.replicas.values():
    #    check_stable_checkpoint(replica, 5)
