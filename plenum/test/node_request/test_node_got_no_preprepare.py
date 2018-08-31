import pytest

from plenum.test.node_request.helper import nodes_last_ordered_equal
from stp_core.loop.eventually import eventually

from plenum.test.helper import sdk_send_batches_of_random_and_check, sdk_send_batches_of_random
from plenum.test.malicious_behaviors_node import router_dont_accept_messages_from, reset_router_accepting

from plenum.test.checkpoints.conftest import chkFreqPatched, reqs_for_checkpoint


@pytest.fixture(scope="module")
def tconf(tconf):
    OLD_DELTA_3PC_ASKING = tconf.DELTA_3PC_ASKING
    tconf.DELTA_3PC_ASKING = 8
    yield tconf
    tconf.DELTA_3PC_ASKING = OLD_DELTA_3PC_ASKING


def test_1_node_got_no_preprepare(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  tconf):
    master_node = txnPoolNodeSet[0]
    behind_node = txnPoolNodeSet[-1]
    last_ordered = master_node.master_last_ordered_3PC[1]
    delta = tconf.DELTA_3PC_ASKING
    num_of_batches = 1

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert behind_node.master_last_ordered_3PC == \
           master_node.master_last_ordered_3PC

    # Emulate connection problems, behind_node doesnt receive pre-prepares
    router_dont_accept_messages_from(behind_node, master_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert len(txnPoolNodeSet[1].master_replica.prePrepares) - 1 == \
           len(behind_node.master_replica.prePrepares)
    assert master_node.master_last_ordered_3PC[1] == last_ordered + num_of_batches * 2
    with pytest.raises(AssertionError):
        nodes_last_ordered_equal(behind_node, master_node)

    # behind_node has requested preprepare and wouldn't request it again until
    # income preprepare seq_no > last_ordered seq_no + DELTA_3PC_ASKING

    # Remove connection problems
    reset_router_accepting(behind_node)

    # Send txns
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)

    # behind_node stashing new 3pc messages and not ordering and not participating in consensus
    assert len(behind_node.master_replica.prePreparesPendingPrevPP) == 1
    with pytest.raises(AssertionError):
        nodes_last_ordered_equal(behind_node, master_node)

    # After DELTA_3PC_ASKING batches, behind_node asks for pre-prepare and starting ordering
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, delta, delta)

    looper.run(eventually(nodes_last_ordered_equal, behind_node, master_node))


def test_2_node_got_no_preprepare(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  tconf,
                                  chkFreqPatched,
                                  reqs_for_checkpoint
                                  ):
    reqs_for_checkpoint *= 2
    master_node = txnPoolNodeSet[0]
    behind_nodes = txnPoolNodeSet[-2:]
    last_ordered = master_node.master_last_ordered_3PC[1]
    delta = tconf.DELTA_3PC_ASKING
    num_of_batches = 1

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    # assert nodes_last_ordered_equal(*behind_nodes, master_node)

    # Emulate connection problems, behind_node doesnt receive pre-prepares
    router_dont_accept_messages_from(behind_nodes[0], master_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    # assert len(txnPoolNodeSet[1].master_replica.prePrepares) - 1 == \
    #        len(behind_nodes[0].master_replica.prePrepares)
    # assert master_node.master_last_ordered_3PC[1] == last_ordered + num_of_batches * 2
    # assert not nodes_last_ordered_equal(behind_nodes[0], master_node)

    # behind_node has requested preprepare and wouldn't request it again until
    # income preprepare seq_no > last_ordered seq_no + DELTA_3PC_ASKING

    # Remove connection problems
    reset_router_accepting(behind_nodes[0])

    # Send txns
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)

    # behind_node stashing new 3pc messages and not ordering and not participating in consensus
    # assert len(behind_nodes[0].master_replica.prePreparesPendingPrevPP) == 1
    # assert not nodes_last_ordered_equal(behind_nodes[0], master_node)

    # Emulate connection problems, behind_node doesnt receive pre-prepares
    router_dont_accept_messages_from(behind_nodes[1], master_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)

    # Remove connection problems
    reset_router_accepting(behind_nodes[1])

    # Pool stayed alive
    # looper.run(eventually(nodes_last_ordered_equal, behind_nodes[1], master_node))

    # Send txns
    sdk_send_batches_of_random(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)

    # After DELTA_3PC_ASKING batches, behind_node asks for pre-prepare and starting ordering
    sdk_send_batches_of_random(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, delta, delta)

    looper.run(eventually(nodes_last_ordered_equal, *behind_nodes, master_node))
