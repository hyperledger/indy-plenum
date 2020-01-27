import pytest

from plenum.test.delayers import cDelay, ppDelay, pDelay, icDelay, msg_rep_delay, vc_delay, nv_delay
from plenum.test.helper import waitForViewChange, checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually


nodeCount = 7
CHK_SIZE = 3


@pytest.fixture(scope="module")
def tconf(tconf):

    old_chk_freq = tconf.CHK_FREQ
    old_log_size = tconf.LOG_SIZE
    old_max_b_size = tconf.Max3PCBatchSize

    tconf.CHK_FREQ = CHK_SIZE
    tconf.LOG_SIZE = 3 * CHK_SIZE
    tconf. Max3PCBatchSize = 1

    yield tconf

    tconf.Max3PCBatchSize = old_max_b_size
    tconf.CHK_FREQ = old_chk_freq
    tconf.LOG_SIZE = old_log_size


def test_finish_view_change_with_incorrect_primaries_list(looper, txnPoolNodeSet, sdk_pool_handle,
                                                          sdk_wallet_steward, tdir, tconf, allPluginsPath):
    """
    This test imitates situation when one of nodes is lagged.
    It missed txn for adding new node and view_change after this.
    After that lagged node started the next view_change with other nodes,
    but it has different committed node_reg and selected other primaries.
    In this case we expect, that lagged node will complete view_change with other primaries
    and will start catchup by Checkpoints because will not be able to ordering.

    """
    def complete_vc(node):
        assert not node.view_change_in_progress

    view_no = checkViewNoForNodes(txnPoolNodeSet)

    # Delta is lagged
    lagging_node = txnPoolNodeSet[3]
    fast_nodes = txnPoolNodeSet[:3] + txnPoolNodeSet[4:]

    # Force 5 view changes so that we have viewNo == 5 and Zeta is the primary.
    for _ in range(5):
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, view_no + 1)
        ensureElectionsDone(looper, txnPoolNodeSet)
        view_no = checkViewNoForNodes(txnPoolNodeSet)

    with delay_rules_without_processing(lagging_node.nodeIbStasher,
                                        msg_rep_delay(),
                                        icDelay(),
                                        vc_delay(),
                                        nv_delay(),
                                        cDelay(),
                                        ppDelay(),
                                        pDelay()):

        # Add new node and this action should starts view_change because of NODE txn ordered
        _, theta = sdk_add_new_steward_and_node(looper, sdk_pool_handle, sdk_wallet_steward,
                                                'Theta_Steward', 'Theta',
                                                tdir, tconf, allPluginsPath=allPluginsPath)
        txnPoolNodeSet.append(theta)
        fast_nodes.append(theta)

        looper.run(checkNodesConnected(fast_nodes))
        ensure_all_nodes_have_same_data(looper, fast_nodes)

        waitForViewChange(looper, fast_nodes, view_no + 1)
        ensureElectionsDone(looper, fast_nodes)

    assert lagging_node.viewNo != fast_nodes[0].viewNo
    assert fast_nodes[0].viewNo == view_no + 1

    current_view_no = checkViewNoForNodes(fast_nodes)
    expected_view_no = current_view_no + 1
    trigger_view_change(txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, expected_view_no)
    ensureElectionsDone(looper, fast_nodes)

    looper.run(eventually(complete_vc, lagging_node, timeout=60))
    assert lagging_node.viewNo == expected_view_no

    # We assume that after 2 Checkpoints receiving lagged node will start catchup and elect right primaries

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 2 * CHK_SIZE)
    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
