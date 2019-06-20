import pytest

from plenum.common.startable import Mode
from plenum.test import waits
from plenum.test.delayers import cDelay
from plenum.test.node_catchup.helper import waitNodeDataEquality, ensure_all_nodes_have_same_data
from plenum.test.helper import sdk_send_random_requests, check_last_ordered_3pc_on_master, \
    sdk_send_random_and_check, sdk_get_replies, max_3pc_batch_limits, assert_eq
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=10) as tconf:
        yield tconf


def test_catchup_during_3pc(tconf, looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    '''
    1) Send 1 3PC batch + 2 reqs
    2) Delay commits on one node
    3) Make sure the batch is ordered on all nodes except the lagged one
    4) start catchup of the lagged node
    5) Make sure that all nodes are equal
    6) Send more requests that we have 3 batches in total
    7) Make sure that all nodes are equal
    '''

    lagging_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]

    with delay_rules(lagging_node.nodeIbStasher, cDelay()):
        sdk_reqs = sdk_send_random_requests(looper, sdk_pool_handle,
                                            sdk_wallet_client, tconf.Max3PCBatchSize + 2)

        looper.run(
            eventually(check_last_ordered_3pc_on_master, rest_nodes, (0, 1))
        )

        lagging_node.start_catchup()

        looper.run(
            eventually(
                lambda: assert_eq(lagging_node.mode, Mode.participating), retryWait=1,
                timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))
            )
        )

        waitNodeDataEquality(looper, *txnPoolNodeSet, customTimeout=5)

    sdk_get_replies(looper, sdk_reqs)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 2 * tconf.Max3PCBatchSize - 2)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
