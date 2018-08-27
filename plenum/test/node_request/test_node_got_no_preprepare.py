import pytest

from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.malicious_behaviors_node import RouterDontAcceptMessagesFrom, resetRouterAccepting


@pytest.fixture(scope="module")
def tconf(tconf):
    OLD_DELTA_3PC_ASKING = tconf.DELTA_3PC_ASKING
    tconf.DELTA_3PC_ASKING = 8
    yield tconf
    tconf.DELTA_3PC_ASKING = OLD_DELTA_3PC_ASKING


def test_1_node_got_no_preprepare(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client):
    master_node = txnPoolNodeSet[0]
    behind_node = txnPoolNodeSet[-1]
    num_of_batches = 1

    # Nodes order batches
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert behind_node.master_last_ordered_3PC == \
           master_node.master_last_ordered_3PC

    # Emulate connection problems, behind_node doesnt receive pre-prepares
    RouterDontAcceptMessagesFrom(behind_node, master_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    assert len(txnPoolNodeSet[1].master_replica.prePrepares) == 2
    assert len(behind_node.master_replica.prePrepares) == 1
    assert master_node.master_last_ordered_3PC[1] == num_of_batches * 2
    assert behind_node.master_last_ordered_3PC[1] + num_of_batches == \
           master_node.master_last_ordered_3PC[1]

    # behind_node has requested preprepare and wouldn't request it again until
    # income preprepare seq_no > last_ordered seq_no + DELTA_3PC_ASKING

    # Remove connection problems
    resetRouterAccepting(behind_node)

    # Send txns and wait for some time
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, num_of_batches)
    looper.runFor(5)

    # behind_node stashing new 3pc messages and not ordering and not participating in consensus
    assert len(behind_node.master_replica.prePreparesPendingPrevPP) == 1
    assert behind_node.master_last_ordered_3PC[1] + num_of_batches * 2 == \
           master_node.master_last_ordered_3PC[1]

    # After DELTA_3PC_ASKING batches, behind_node asks for pre-prepare and starting ordering
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, 3)
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 3, 3)
    looper.runFor(5)
    assert behind_node.master_last_ordered_3PC[1] == \
           master_node.master_last_ordered_3PC[1]
