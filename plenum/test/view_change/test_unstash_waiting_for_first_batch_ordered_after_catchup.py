import pytest

from plenum.common.constants import PREPARE, PREPREPARE
from plenum.test.delayers import cDelay, msg_rep_delay
from plenum.test.helper import sdk_send_random_and_check, assertExp, sdk_send_random_requests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.propagate.helper import recvdPrePrepareForInstId
from plenum.test.stasher import delay_rules, delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances, \
    check_not_in_view_change
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

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


def test_unstash_waiting_for_first_batch_ordered_after_catchup(
        looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle, tconf):
    lagged_node = txnPoolNodeSet[-1]
    other_nodes = list(set(txnPoolNodeSet) - {lagged_node})
    other_stashers = [n.nodeIbStasher for n in other_nodes]

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    last_ordered_lagged_before = lagged_node.master_last_ordered_3PC
    # do not process any message reqs for PrePrepares
    with delay_rules_without_processing(lagged_node.nodeIbStasher, msg_rep_delay(types_to_delay=[PREPARE, PREPREPARE])):
        with delay_rules(lagged_node.nodeIbStasher, cDelay()):
            ensure_view_change(looper, txnPoolNodeSet)
            looper.run(eventually(check_not_in_view_change, txnPoolNodeSet))
            ensureElectionsDone(looper, other_nodes,
                                instances_list=range(getRequiredInstances(len(txnPoolNodeSet))))

            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_client, 1)

            # delay Commits on all nodes so that there are some PrePrepares still stashed after catchup
            with delay_rules(other_stashers, cDelay()):
                pre_prep_before = len(recvdPrePrepareForInstId(lagged_node, 0))
                sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 2)
                # wait till lagged node recives the new PrePrepares
                # they will be stashed as WAITING_FIRST_BATCH_IN_VIEW
                looper.run(
                    eventually(lambda: assertExp(len(recvdPrePrepareForInstId(lagged_node, 0)) == pre_prep_before + 2)))

                # catchup the lagged node
                # the latest 2 PrePrepares are still stashed
                lagged_node.start_catchup()
                looper.run(
                    eventually(lambda: assertExp(lagged_node.master_last_ordered_3PC > last_ordered_lagged_before)))

            sdk_send_random_and_check(looper, txnPoolNodeSet,
                                      sdk_pool_handle, sdk_wallet_client, 2)

    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=30)
