import pytest

from plenum.test.checkpoints.helper import check_stashed_chekpoints
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.helper import nodes_last_ordered_equal

from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.malicious_behaviors_node import dont_send_prepare_and_commit_to, reset_sending

from plenum.test.checkpoints.conftest import chkFreqPatched


@pytest.fixture(scope="module")
def tconf(tconf):
    OldMax3PCBatchSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = OldMax3PCBatchSize


def test_2_nodes_get_only_preprepare(looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client,
                                     tconf,
                                     chkFreqPatched):
    # CHK_FREQ = 2 in this test
    # num of stashed checkpoints to start catchup is 2 (or 4 batches)

    master_node = txnPoolNodeSet[0]
    behind_nodes = txnPoolNodeSet[-2:]

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1, 1)
    nodes_last_ordered_equal(*txnPoolNodeSet)

    # Emulate connection problems, 1st behind_node receiving only pre-prepares
    dont_send_prepare_and_commit_to(txnPoolNodeSet[:-2], behind_nodes[0].name)

    # Send some txns and 1st behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1, 1)
    assert behind_nodes[0].master_last_ordered_3PC[1] + 1 == \
           master_node.master_last_ordered_3PC[1]

    # 1st behind got 1 stashed checkpoint from each node
    check_stashed_chekpoints(behind_nodes[0], 3)

    # Remove connection problems
    reset_sending(txnPoolNodeSet[:-2])

    # Send txns
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1, 1)

    # 1st behind_node is getting new prepares, but still can't order,
    # cause can't get quorum for prepare for previous batch
    assert behind_nodes[0].master_last_ordered_3PC[1] + 1 * 2 == \
           master_node.master_last_ordered_3PC[1]

    # Emulate connection problems, 2nd behind_node receiving only pre-prepares
    dont_send_prepare_and_commit_to(txnPoolNodeSet[:-2], behind_nodes[1].name)

    # Send some txns and 2nd behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1, 1)
    assert behind_nodes[1].master_last_ordered_3PC[1] + 1 == \
           master_node.master_last_ordered_3PC[1]

    # 2d behind got 1 stashed checkpoint from all nodes except 1st behind
    check_stashed_chekpoints(behind_nodes[1], 2)

    # 1st behind got another stashed checkpoint, so should catch-up now
    waitNodeDataEquality(looper, master_node, behind_nodes[0], customTimeout=60,
                         exclude_from_check=['check_last_ordered_3pc_backup'])

    # Remove connection problems
    reset_sending(txnPoolNodeSet[:-2])

    # Send txns
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1, 1)

    # 2nd behind_node is getting new prepares, but still can't order,
    # cause can't get quorum for prepare for previous batch
    assert behind_nodes[1].master_last_ordered_3PC[1] + 1 * 2 == \
           master_node.master_last_ordered_3PC[1]

    # After achieving stable checkpoint, behind_node start ordering
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                         1, 1)
    # 2d behind got another stashed checkpoint, so should catch-up now
    waitNodeDataEquality(looper, master_node, behind_nodes[1], customTimeout=60,
                         exclude_from_check=['check_last_ordered_3pc_backup'])

    # Pool is working
    waitNodeDataEquality(looper, master_node, *behind_nodes, customTimeout=5,
                         exclude_from_check=['check_last_ordered_3pc_backup'])
