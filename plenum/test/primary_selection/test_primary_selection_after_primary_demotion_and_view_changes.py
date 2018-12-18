from plenum.common.util import hexToFriendly

from stp_core.common.log import getlogger

from plenum.test.pool_transactions.helper import sdk_send_update_node

from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change_complete

logger = getlogger()


def test_primary_selection_after_primary_demotion_and_view_changes(looper, txnPoolNodeSet,
                                                                   sdk_pool_handle,
                                                                   sdk_wallet_steward,
                                                                   txnPoolMasterNodes):
    """
    Demote primary and do multiple view changes forcing primaries rotation.
    Demoted primary should be skipped without additional view changes.
    """

    viewNo0 = checkViewNoForNodes(txnPoolNodeSet)

    logger.info("1. turn off the node which has primary replica for master instanse, "
                " this should trigger view change")
    master_node = txnPoolMasterNodes[0]
    node_dest = hexToFriendly(master_node.nodestack.verhex)
    sdk_send_update_node(looper, sdk_wallet_steward,
                         sdk_pool_handle,
                         node_dest, master_node.name,
                         None, None,
                         None, None,
                         services=[])

    restNodes = [node for node in txnPoolNodeSet \
                 if node.name != master_node.name]
    ensureElectionsDone(looper, restNodes)

    viewNo1 = checkViewNoForNodes(restNodes)

    assert viewNo1 == viewNo0 + 1
    assert master_node.viewNo == viewNo0
    assert len(restNodes[0].replicas) == 1  # only one instance left
    assert restNodes[0].replicas[0].primaryName != master_node.name

    # ensure pool is working properly
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)

    logger.info("2. force view change 2 and check final viewNo")
    ensure_view_change_complete(looper, restNodes)

    viewNo2 = checkViewNoForNodes(restNodes)
    assert restNodes[0].replicas[0].primaryName != master_node.name
    assert viewNo2 == viewNo1 + 1

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)

    logger.info("3. force view change 3 and check final viewNo")
    ensure_view_change_complete(looper, restNodes)
    viewNo3 = checkViewNoForNodes(restNodes)
    assert restNodes[0].replicas[0].primaryName != master_node.name
    assert viewNo3 == viewNo2 + 1

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)

    logger.info("4. force view change 4 and check final viewNo")
    ensure_view_change_complete(looper, restNodes)
    viewNo4 = checkViewNoForNodes(restNodes)
    assert restNodes[0].replicas[0].primaryName != master_node.name
    assert viewNo4 == viewNo3 + 1

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 3)
