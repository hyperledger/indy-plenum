from plenum.test.checkpoints.helper import check_for_nodes, check_stable_checkpoint, check_received_checkpoint_votes
from plenum.test.delayers import ppDelay, msg_rep_delay
from plenum.test.helper import sdk_send_random_and_check, assertExp
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually

CHK_FREQ = 5
nodeCount = 7  # it's crucial for this test to have f > 1


def test_stabilize_checkpoint_while_unstashing_when_missing_pre_prepare(looper,
                                                                        chkFreqPatched,
                                                                        reqs_for_checkpoint,
                                                                        txnPoolNodeSet,
                                                                        sdk_pool_handle,
                                                                        sdk_wallet_client):
    # Prepare nodes
    lagging_node = txnPoolNodeSet[-1]
    lagging_master_replcia = lagging_node.master_replica
    rest_nodes = txnPoolNodeSet[:-1]

    # 1. send enough requests so that just 1 is left for checkpoint stabilization
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, reqs_for_checkpoint - 1)

    # 2. delay PrePrepare on 1 node so that prepares and commits will be stashed
    with delay_rules(lagging_node.nodeIbStasher, ppDelay()):
        with delay_rules(lagging_node.nodeIbStasher, msg_rep_delay()):
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                      sdk_wallet_client, 1)

            # all good nodes stabilized checkpoint
            looper.run(eventually(check_for_nodes, rest_nodes, check_stable_checkpoint, 5))

            # bad node received checkpoints from all nodes but didn't stabilize it
            looper.run(eventually(check_for_nodes, [lagging_node], check_stable_checkpoint, 0))
            looper.run(eventually(check_for_nodes, [lagging_node], check_received_checkpoint_votes, 5, len(rest_nodes)))

            # bad node has all commits and prepares for the last request stashed
            looper.run(eventually(
                lambda: assertExp(
                    (0, CHK_FREQ) in lagging_master_replcia._ordering_service.preparesWaitingForPrePrepare and
                    len(lagging_master_replcia._ordering_service.preparesWaitingForPrePrepare[(0, CHK_FREQ)]) == len(rest_nodes) - 1
                )
            ))
            looper.run(eventually(
                lambda: assertExp(
                    (0, CHK_FREQ) in lagging_master_replcia._ordering_service.commitsWaitingForPrepare and
                    len(lagging_master_replcia._ordering_service.commitsWaitingForPrepare[(0, CHK_FREQ)]) == len(rest_nodes)
                )
            ))

    # 3. the delayed PrePrepare is processed, and stashed prepares and commits are unstashed
    # checkpoint will be stabilized during unstashing, and the request will be ordered
    looper.run(eventually(check_for_nodes, [lagging_node], check_stable_checkpoint, 5))
    waitNodeDataEquality(looper, *txnPoolNodeSet, customTimeout=5)
