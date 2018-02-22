from plenum.test.helper import sdk_send_random_and_check, checkViewNoForNodes
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.conftest import looper
from plenum.test.view_change.helper import ensure_view_change_complete


def test_last_ordered_3pc_reset_if_more_than_new_view(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Check that if last_ordered_3pc's viewNo on a Replica is greater than the new viewNo after view change,
    then last_ordered_3pc is reset to (0,0).
    It can be that last_ordered_3pc was set for the previous view, since it's set during catch-up

    Example: a Node has last_ordered = (1, 300), and then the whole pool except this node restarted.
    The new viewNo is 0, but last_ordered is (1, 300), so all new requests will be discarded by this Node
    if we don't reset last_ordered_3pc
    """
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        node.master_replica.last_ordered_3pc = (old_view_no + 2, 100)

    ensure_view_change_complete(looper, txnPoolNodeSet, customTimeout=60)
    view_no = checkViewNoForNodes(txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert (view_no, 0) == node.master_replica.last_ordered_3pc

    # Make sure the pool is working
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


def test_last_ordered_3pc_reset_if_equal_to_new_view(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Check that if last_ordered_3pc's viewNo on a Replica is equal to the new viewNo after view change,
    then last_ordered_3pc is reset to (0,0).
    It can be that last_ordered_3pc was set for the previous view, since it's set during catch-up

    Example: a Node has last_ordered = (1, 300), and then the whole pool except this node restarted.
    The new viewNo is 0, but last_ordered is (1, 300), so all new requests will be discarded by this Node
    if we don't reset last_ordered_3pc
    """
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        node.master_replica.last_ordered_3pc = (old_view_no + 1, 100)

    ensure_view_change_complete(looper, txnPoolNodeSet, customTimeout=60)
    view_no = checkViewNoForNodes(txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert (view_no, 0) == node.master_replica.last_ordered_3pc

    # Make sure the pool is working
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


def test_last_ordered_3pc_not_reset_if_less_than_new_view(txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    Check that if last_ordered_3pc's viewNo on a Replica is equal to the new viewNo after view change,
    then last_ordered_3pc is reset to (0,0).
    It can be that last_ordered_3pc was set for the previous view, since it's set during catch-up

    Example: a Node has last_ordered = (1, 300), and then the whole pool except this node restarted.
    The new viewNo is 0, but last_ordered is (1, 300), so all new requests will be discarded by this Node
    if we don't reset last_ordered_3pc
    """
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        node.master_replica.last_ordered_3pc = (old_view_no, 100)

    ensure_view_change_complete(looper, txnPoolNodeSet, customTimeout=60)

    for node in txnPoolNodeSet:
        assert (old_view_no, 100) == node.master_replica.last_ordered_3pc

    # Make sure the pool is working
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
