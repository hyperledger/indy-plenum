from plenum.test.helper import sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import demote_node
from plenum.test.view_change.helper import ensure_view_change_by_primary_restart
from stp_core.common.log import getlogger

logger = getlogger()


def demote_primary_node(looper,
                        initial_pool_of_nodes,
                        pool_of_nodes,
                        sdk_pool_handle,
                        sdk_wallet_stewards):
    demoted_node = [node for node in pool_of_nodes if node.has_master_primary][0]
    indx = initial_pool_of_nodes.index(demoted_node)
    demote_node(looper, sdk_wallet_stewards[indx],
                sdk_pool_handle, demoted_node)
    pool_of_nodes = list(set(pool_of_nodes) - {demoted_node})

    return pool_of_nodes


def test_restart_primaries_then_demote(
        looper, txnPoolNodeSet,
        tconf, tdir, allPluginsPath,
        sdk_pool_handle,
        sdk_wallet_stewards):
    """
    """
    sdk_wallet_steward = sdk_wallet_stewards[0]
    logger.info("1. Restart Node1")
    pool_of_nodes = ensure_view_change_by_primary_restart(looper,
                                                          txnPoolNodeSet,
                                                          tconf,
                                                          tdir,
                                                          allPluginsPath,
                                                          customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT,
                                                          exclude_from_check=['check_last_ordered_3pc_backup'])

    # ensure pool is working properly
    sdk_send_random_and_check(looper, pool_of_nodes, sdk_pool_handle,
                              sdk_wallet_steward, 1)

    logger.info("2. Restart Node2")
    pool_of_nodes = ensure_view_change_by_primary_restart(looper,
                                                          pool_of_nodes,
                                                          tconf,
                                                          tdir,
                                                          allPluginsPath,
                                                          customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT,
                                                          exclude_from_check=['check_last_ordered_3pc_backup'])

    # ensure pool is working properly
    sdk_send_random_and_check(looper, pool_of_nodes, sdk_pool_handle,
                              sdk_wallet_steward, 1)

    logger.info("3. Demote Node3")
    # demote the node
    pool_of_nodes = demote_primary_node(looper,
                                        txnPoolNodeSet,
                                        pool_of_nodes,
                                        sdk_pool_handle,
                                        sdk_wallet_stewards)

    # make sure view changed
    waitForViewChange(looper, pool_of_nodes, expectedViewNo=3)

    # ensure pool is working properly
    sdk_send_random_and_check(looper, pool_of_nodes, sdk_pool_handle,
                              sdk_wallet_steward, 10)
    ensure_all_nodes_have_same_data(looper, nodes=pool_of_nodes)
