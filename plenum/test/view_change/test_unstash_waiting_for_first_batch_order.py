import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 4


@pytest.fixture(scope="module")
def tconf(tconf):
    old_chk = tconf.CHK_FREQ
    old_log = tconf.LOG_SIZE
    old_b_size = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    tconf.CHK_FREQ = 5
    tconf.LOG_SIZE = 5 * 3
    yield tconf
    tconf.CHK_FREQ = old_chk
    tconf.LOG_SIZE = old_log
    tconf.Max3PCBatchSize = old_b_size


def test_unstash_waiting_for_first_batch_ordered(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle, tconf):
    lagged_node = txnPoolNodeSet[-1]
    other_nodes = list(set(txnPoolNodeSet) - {lagged_node})

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    with delay_rules(lagged_node.nodeIbStasher, cDelay()):
        ensure_view_change(looper, txnPoolNodeSet)
        ensureElectionsDone(looper, other_nodes,
                            instances_list=range(getRequiredInstances(len(txnPoolNodeSet))))

        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 2)

    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
