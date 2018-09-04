import pytest

from plenum.test.node_request.helper import nodes_last_ordered_equal
from stp_core.loop.eventually import eventually

from plenum.test.helper import sdk_send_batches_of_random_and_check, sdk_send_batches_of_random
from plenum.test.malicious_behaviors_node import router_dont_accept_messages_from, reset_router_accepting

from plenum.test.checkpoints.conftest import chkFreqPatched


def tconf(tconf):
    OldMax3PCBatchSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 2
    yield tconf
    tconf.Max3PCBatchSize = OldMax3PCBatchSize


def test_1_node_got_no_preprepare(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  tconf,
                                  chkFreqPatched):
    master_node = txnPoolNodeSet[0]
    behind_node = txnPoolNodeSet[-1]
    delta = tconf.CHK_FREQ * 3
    num_of_batches = 1

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)
    assert behind_node.master_last_ordered_3PC == \
           master_node.master_last_ordered_3PC

    # Emulate connection problems, behind_node doesnt receive pre-prepares
    router_dont_accept_messages_from(behind_node, master_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)
    with pytest.raises(AssertionError):
        nodes_last_ordered_equal(behind_node, master_node)

    # behind_node has requested preprepare and wouldn't request it again.
    # It will catchup with closest stable checkpoint

    # Remove connection problems
    reset_router_accepting(behind_node)

    # Send txns
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)

    # behind_node stashing new 3pc messages and not ordering and not participating in consensus
    assert len(behind_node.master_replica.prePreparesPendingPrevPP) == 1
    with pytest.raises(AssertionError):
        nodes_last_ordered_equal(behind_node, master_node)

    # After achieving stable checkpoint, behind_node start ordering
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, delta, delta)

    # Pool is working
    looper.run(eventually(nodes_last_ordered_equal, behind_node, master_node))


def test_2_node_got_no_preprepare(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  tconf,
                                  chkFreqPatched):
    master_node = txnPoolNodeSet[0]
    behind_nodes = txnPoolNodeSet[-2:]
    delta = tconf.CHK_FREQ * 3
    num_of_batches = 1

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)
    nodes_last_ordered_equal(*behind_nodes, master_node)

    # Emulate connection problems, behind_node doesnt receive pre-prepares
    router_dont_accept_messages_from(behind_nodes[0], master_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)

    with pytest.raises(AssertionError):
        nodes_last_ordered_equal(behind_nodes[0], master_node)

    # behind_node has requested preprepare and wouldn't request it again.
    # It will catchup with closest stable checkpoint

    # Remove connection problems
    reset_router_accepting(behind_nodes[0])

    # Send txns
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)

    # behind_node stashing new 3pc messages and not ordering and not participating in consensus
    assert len(behind_nodes[0].master_replica.prePreparesPendingPrevPP) == 1
    with pytest.raises(AssertionError):
        nodes_last_ordered_equal(behind_nodes[0], master_node)

    # Emulate connection problems, behind_node doesnt receive pre-prepares
    router_dont_accept_messages_from(behind_nodes[1], master_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)

    # Remove connection problems
    reset_router_accepting(behind_nodes[1])

    # Pool stayed alive
    looper.run(eventually(nodes_last_ordered_equal, behind_nodes[1], master_node))

    # Send txns
    sdk_send_batches_of_random(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)

    # After achieving stable checkpoint, behind_node start ordering
    sdk_send_batches_of_random(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, delta, delta)

    # Pool is working
    looper.run(eventually(nodes_last_ordered_equal, *behind_nodes, master_node))
