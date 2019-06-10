import pytest

from plenum.common.messages.node_messages import Commit
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import delay_3pc
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits, assert_eq, sdk_send_random_requests, \
    sdk_get_replies, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules_without_processing, delay_rules
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        old_cons_proof_timeout = tconf.ConsistencyProofsTimeout

        # Effectively disable resending cons proof requests after timeout
        tconf.ConsistencyProofsTimeout = 1000

        yield tconf
        tconf.ConsistencyProofsTimeout = old_cons_proof_timeout


def test_catchup_from_unequal_nodes_without_waiting(looper,
                                                    txnPoolNodeSet,
                                                    sdk_pool_handle,
                                                    sdk_wallet_client):
    normal_node = txnPoolNodeSet[0]
    lagging_node_1 = txnPoolNodeSet[1]
    lagging_node_2 = txnPoolNodeSet[2]
    stopped_node = txnPoolNodeSet[3]

    # Make sure everyone have one batch
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

    # Wait until all nodes have same data and store last 3PC number of node that's going to be "stopped"
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=30)
    last_3pc = stopped_node.master_last_ordered_3PC

    with delay_rules_without_processing(stopped_node.nodeIbStasher, delay_3pc()):
        # Create one more batch on all nodes except "stopped" node
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

        with delay_rules(lagging_node_1.nodeIbStasher, delay_3pc(msgs=Commit)):
            # Create one more batch on all nodes except "stopped" and first lagging node
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)

            with delay_rules(lagging_node_2.nodeIbStasher, delay_3pc(msgs=Commit)):
                # Create one more batch on all nodes except "stopped" and both lagging nodes
                # This time we can't wait for replies because there will be only one
                reqs = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 1)

                # Wait until normal node orders txn
                looper.run(eventually(lambda: assert_eq(normal_node.master_last_ordered_3PC[1],
                                                        last_3pc[1] + 3)))

                # Now all nodes have different number of txns, so if we try to start a catch up
                # it is guaranteed that we'll need to ask for equal consistency proofs, and
                # disabled timeout ensures that node can do so without relying on timeout
                stopped_node.start_catchup()

                # Wait until catchup ends
                looper.run(eventually(lambda: assert_eq(stopped_node.ledgerManager._node_leecher._state,
                                                        NodeLeecherService.State.Idle)))

                # Ensure stopped node caught up at least one batch
                assert stopped_node.master_last_ordered_3PC[1] > last_3pc[1]

                # And there was no view change
                assert stopped_node.master_last_ordered_3PC[0] == last_3pc[0]

            # Make sure replies from last request are eventually received
            sdk_get_and_check_replies(looper, reqs)
