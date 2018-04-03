from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.view_change.helper import view_change_in_between_3pc


def slow_nodes(node_set):
    return [replica.node for replica in getNonPrimaryReplicas(node_set)[1:]]


def test_view_change_in_between_3pc_2_of_4_nodes_with_non_primary(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    - Slow processing 3PC messages for 2 of 4 node (2>f).
    - Both nodes are non-primary for master neither in this nor the next view
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet,
                               slow_nodes(txnPoolNodeSet),
                               sdk_pool_handle, sdk_wallet_client)


def test_view_change_in_between_3pc_2_of_4_nodes_with_non_primary_long_delay(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    - Slow processing 3PC messages for 2 of 4 node (2>f).
    - Both nodes are non-primary for master neither in this nor the next view
    - do view change
    """
    view_change_in_between_3pc(looper, txnPoolNodeSet,
                               slow_nodes(txnPoolNodeSet),
                               sdk_pool_handle, sdk_wallet_client,
                               slow_delay=20)
