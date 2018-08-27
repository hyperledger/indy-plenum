import pytest

from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.malicious_behaviors_node import dontSendPrepareAndCommitTo, resetSending


@pytest.fixture(scope="module")
def tconf(tconf):
    OLD_DELTA_3PC_ASKING = tconf.DELTA_3PC_ASKING
    tconf.DELTA_3PC_ASKING = 8
    yield tconf
    tconf.DELTA_3PC_ASKING = OLD_DELTA_3PC_ASKING


def test_1_node_get_only_preprepare(looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client):
    master_node = txnPoolNodeSet[0]
    behind_node = txnPoolNodeSet[-1]
    last_ordered = master_node.master_last_ordered_3PC[1]
    num_of_batches = 1

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert behind_node.master_last_ordered_3PC == \
           master_node.master_last_ordered_3PC

    # Emulate connection problems, behind_node receiving only pre-prepares
    dontSendPrepareAndCommitTo(txnPoolNodeSet[:-1], behind_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert master_node.master_last_ordered_3PC[1] == last_ordered + num_of_batches * 2
    assert behind_node.master_last_ordered_3PC[1] + num_of_batches == \
           master_node.master_last_ordered_3PC[1]

    # Remove connection problems
    resetSending(txnPoolNodeSet[:-1])

    # Send txns and wait for some time
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    looper.runFor(5)

    # behind_node is getting new prepares, but still can't order,
    # cause can't get quorum for prepare for previous batch
    assert len(behind_node.master_replica.prepares[(0, last_ordered + num_of_batches * 2)].voters) == 1
    assert len(behind_node.master_replica.prepares[(0, last_ordered + num_of_batches * 3)].voters) == 3
    assert behind_node.master_last_ordered_3PC[1] + num_of_batches * 2 == \
           master_node.master_last_ordered_3PC[1]

    # When we try to order commit, which seq_no > DELTA_3PC_ASKING + last_ordered of ours,
    # than we requesting 3pc messages for last_ordered seq_no + 1
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 6, 6)
    looper.runFor(5)
    assert behind_node.master_last_ordered_3PC[1] == \
           master_node.master_last_ordered_3PC[1]


def test_2_nodes_get_only_preprepare(looper,
                                     txnPoolNodeSet,
                                     sdk_pool_handle,
                                     sdk_wallet_client):
    master_node = txnPoolNodeSet[0]
    behind_nodes = txnPoolNodeSet[-2:]
    last_ordered = master_node.master_last_ordered_3PC[1]
    num_of_batches = 1

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert behind_nodes[0].master_last_ordered_3PC == \
           master_node.master_last_ordered_3PC
    assert behind_nodes[1].master_last_ordered_3PC == \
           master_node.master_last_ordered_3PC

    # Emulate connection problems, 1st behind_node receiving only pre-prepares
    dontSendPrepareAndCommitTo(txnPoolNodeSet[:-2], behind_nodes[0].name)

    # Send some txns and 1st behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert master_node.master_last_ordered_3PC[1] == last_ordered + num_of_batches * 2
    assert behind_nodes[0].master_last_ordered_3PC[1] + num_of_batches == \
           master_node.master_last_ordered_3PC[1]

    # Remove connection problems
    resetSending(txnPoolNodeSet[:-2])

    # Send txns and wait for some time
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    looper.runFor(5)

    # 1st behind_node is getting new prepares, but still can't order,
    # cause can't get quorum for prepare for previous batch
    assert len(behind_nodes[0].master_replica.prepares[(0, last_ordered + num_of_batches * 2)].voters) == 2
    assert len(behind_nodes[0].master_replica.prepares[(0, last_ordered + num_of_batches * 3)].voters) == 3
    assert behind_nodes[0].master_last_ordered_3PC[1] + num_of_batches * 2 == \
           master_node.master_last_ordered_3PC[1]

    # Emulate connection problems, 2nd behind_node receiving only pre-prepares
    dontSendPrepareAndCommitTo(txnPoolNodeSet[:-2], behind_nodes[1].name)

    # Send some txns and 2nd behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert master_node.master_last_ordered_3PC[1] == last_ordered + num_of_batches * 4
    assert behind_nodes[1].master_last_ordered_3PC[1] + num_of_batches == \
           master_node.master_last_ordered_3PC[1]

    # Remove connection problems
    resetSending(txnPoolNodeSet[:-2])

    # Send txns and wait for some time
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    looper.runFor(5)

    # 2nd behind_node is getting new prepares, but still can't order,
    # cause can't get quorum for prepare for previous batch
    assert len(behind_nodes[1].master_replica.prepares[(0, last_ordered + num_of_batches * 4)].voters) == 2
    assert len(behind_nodes[1].master_replica.prepares[(0, last_ordered + num_of_batches * 5)].voters) == 3
    assert behind_nodes[1].master_last_ordered_3PC[1] + num_of_batches * 2 == \
           master_node.master_last_ordered_3PC[1]

    # Now, we have two nodes, that can't order, but participating in consensus,
    # and pool is working
    assert master_node.master_last_ordered_3PC[1] == \
           behind_nodes[0].master_last_ordered_3PC[1] + 4 == \
           behind_nodes[1].master_last_ordered_3PC[1] + 2

    # When we try to order commit, which seq_no is more (at DELTA_3PC_ASKING size)
    # than last ordered of ours, than we requesting for last_ordered seq_no + 1 3pc messages
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 6, 6)
    looper.runFor(5)
    assert master_node.master_last_ordered_3PC[1] == \
           behind_nodes[0].master_last_ordered_3PC[1] == \
           behind_nodes[1].master_last_ordered_3PC[1]
