from plenum.common.constants import ALIAS, SERVICES

from plenum.test.helper import sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import updateNodeData, \
    buildPoolClientAndWallet
from plenum.test.view_change.helper import ensure_view_change_by_primary_restart
from stp_core.common.log import getlogger

logger = getlogger()


def demote_primary_node(looper,
                        initial_pool_of_nodes,
                        pool_of_nodes,
                        poolTxnStewardNames,
                        poolTxnData,
                        tdirWithClientPoolTxns):
    demoted_node = [node for node in pool_of_nodes if node.has_master_primary][0]
    indx = initial_pool_of_nodes.index(demoted_node)
    steward_name = poolTxnStewardNames[indx]
    stewards_seed = poolTxnData["seeds"][steward_name].encode()

    stewardClient, stewardWallet = buildPoolClientAndWallet(
        (steward_name, stewards_seed), tdirWithClientPoolTxns)
    looper.add(stewardClient)
    looper.run(stewardClient.ensureConnectedToNodes())

    node_data = {
        ALIAS: demoted_node.name,
        SERVICES: []
    }
    updateNodeData(looper, stewardClient,
                   stewardWallet, demoted_node, node_data)
    pool_of_nodes = list(set(pool_of_nodes) - {demoted_node})

    return pool_of_nodes


def test_restart_primaries_then_demote(
        looper, txnPoolNodeSet,
        tconf, tdir, allPluginsPath,
        sdk_pool_handle, sdk_wallet_steward,
        poolTxnStewardNames,
        poolTxnData,
        tdirWithClientPoolTxns):
    """
    """
    logger.info("1. Restart Node1")
    pool_of_nodes = ensure_view_change_by_primary_restart(looper,
                                                          txnPoolNodeSet,
                                                          tconf,
                                                          tdir,
                                                          allPluginsPath,
                                                          customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)

    # ensure pool is working properly
    sdk_send_random_and_check(looper, pool_of_nodes, sdk_pool_handle,
                              sdk_wallet_steward, 1)

    logger.info("2. Restart Node2")
    pool_of_nodes = ensure_view_change_by_primary_restart(looper,
                                                          pool_of_nodes,
                                                          tconf,
                                                          tdir,
                                                          allPluginsPath,
                                                          customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)

    # ensure pool is working properly
    sdk_send_random_and_check(looper, pool_of_nodes, sdk_pool_handle,
                              sdk_wallet_steward, 1)

    logger.info("3. Demote Node3")
    # demote the node
    pool_of_nodes = demote_primary_node(looper,
                                        txnPoolNodeSet,
                                        pool_of_nodes,
                                        poolTxnStewardNames,
                                        poolTxnData,
                                        tdirWithClientPoolTxns)

    # make sure view changed
    waitForViewChange(looper, pool_of_nodes, expectedViewNo=3)

    # ensure pool is working properly
    sdk_send_random_and_check(looper, pool_of_nodes, sdk_pool_handle,
                              sdk_wallet_steward, 10)
    ensure_all_nodes_have_same_data(looper, nodes=pool_of_nodes)
