import pytest

from plenum.test.node_request.helper import nodes_last_ordered_equal
from stp_core.loop.eventually import eventually

from plenum.test.helper import sdk_send_batches_of_random_and_check
from plenum.test.malicious_behaviors_node import dont_send_prepare_and_commit_to, reset_sending

from plenum.test.checkpoints.conftest import chkFreqPatched


@pytest.fixture(scope="module")
def tconf(tconf):
    OldMax3PCBatchSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = OldMax3PCBatchSize


def test_1_node_get_only_preprepare(looper,
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

    # Emulate connection problems, behind_node receiving only pre-prepares
    dont_send_prepare_and_commit_to(txnPoolNodeSet[:-1], behind_node.name)

    # Send some txns and behind_node cant order them while pool is working
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)
    # assert behind_node.master_last_ordered_3PC[1] + num_of_batches == \
    #        master_node.master_last_ordered_3PC[1]

    # Remove connection problems
    reset_sending(txnPoolNodeSet[:-1])

    # Send txns
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, num_of_batches, num_of_batches)

    # After achieving stable checkpoint, behind_node start ordering
    sdk_send_batches_of_random_and_check(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, delta, delta)

    # Pool is working
    looper.run(eventually(nodes_last_ordered_equal, behind_node, master_node))
