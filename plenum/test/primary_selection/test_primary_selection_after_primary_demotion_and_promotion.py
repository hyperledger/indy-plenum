from plenum.common.util import hexToFriendly

from plenum.common.constants import VALIDATOR
from plenum.test.pool_transactions.helper import sdk_send_update_node

from plenum.test.test_node import ensureElectionsDone
from plenum.test.helper import sdk_send_random_and_check

from plenum.test.primary_selection.helper import getPrimaryNodesIdxs


def test_primary_selection_after_demoted_primary_node_promotion(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward,
        txnPoolMasterNodes):
    """
    Demote primary of master instance, wait for view change and promote it back.
    Check primaries for instances.
    """
    assert len(txnPoolNodeSet) == 4

    # Check primaries after test setup.
    primariesIdxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    assert len(primariesIdxs) == 2
    assert primariesIdxs[0] == 0
    assert primariesIdxs[1] == 1

    master_node = txnPoolMasterNodes[0]

    # Demote primary of master instance.
    node_dest = hexToFriendly(master_node.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, master_node.name,
                         None, None,
                         None, None,
                         services=[])

    restNodes = [node for node in txnPoolNodeSet if node.name != master_node.name]
    ensureElectionsDone(looper, restNodes)

    # Check that there is only one instance now, check it's primary.
    primariesIdxs = getPrimaryNodesIdxs(restNodes)
    assert len(primariesIdxs) == 1
    assert primariesIdxs[0] == 1

    # Ensure pool is working properly.
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)

    # Promote demoted node back.
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, master_node.name,
                         None, None,
                         None, None,
                         services=[VALIDATOR])

    # Ensure pool is working properly.
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)

    # Check that there are two instances again, check their primaries.
    primariesIdxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    assert len(primariesIdxs) == 2
    assert primariesIdxs[0] == 2
    assert primariesIdxs[1] == 3
