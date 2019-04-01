from plenum.test.view_change.helper import ensure_all_nodes_have_same_data, \
    start_stopped_node
from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState, POOL_LEDGER_ID
from plenum.test.helper import sdk_send_random_and_check

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.node_catchup.helper import check_ledger_state
from plenum.test.test_node import checkNodesConnected
from plenum.test import waits
from plenum.common.startable import Mode
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected

logger = getlogger()


def catchuped(node):
    assert node.mode == Mode.participating


def test_node_catchup_when_3_not_primary_node_restarted(
        looper, txnPoolNodeSet, tdir, tconf,
        allPluginsPath, sdk_wallet_steward, sdk_pool_handle):
    """
    Test case:
    1. Create pool of 4 nodes
    2. Stop not primary node
    3. Send some txns
    4. Start stopped node
    5. Ensure, that restarted node got all txns which was sent during restart
    6. Do step 2-5 for other not primary node in pool
    """

    def start_stop_one_node(node_to_restart, pool_of_nodes):
        """

        :param node_to_restart: node, which would be restarted
        :param pool_of_nodes: current pool
        :return: new pool with restarted node
        Node restart procedure consist of:
        1. Calling stop()
        2. Remove from looper and pool
        3. Create new instance of node with the same ha, cliha and node_name
        (also all path to data, keys and etc would be exactly as for stopped node)
        4. Add new instance into looper and pool
        5. Check, that other nodes accepted new instance and all pool has the same data
        """

        remaining_nodes = list(set(pool_of_nodes) - {node_to_restart})
        disconnect_node_and_ensure_disconnected(looper,
                                                pool_of_nodes,
                                                node_to_restart,
                                                stopNode=True)
        looper.removeProdable(node_to_restart)
        ensure_all_nodes_have_same_data(looper,
                                        remaining_nodes,
                                        custom_timeout=tconf.VIEW_CHANGE_TIMEOUT)
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_steward, 1)
        node_to_restart = start_stopped_node(node_to_restart,
                                             looper,
                                             tconf,
                                             tdir,
                                             allPluginsPath,
                                             delay_instance_change_msgs=True)
        pool_of_nodes = remaining_nodes + [node_to_restart]
        looper.run(checkNodesConnected(pool_of_nodes))
        ensure_all_nodes_have_same_data(looper,
                                        pool_of_nodes,
                                        custom_timeout=tconf.VIEW_CHANGE_TIMEOUT,
                                        exclude_from_check=['check_last_ordered_3pc_backup'])
        timeout = waits.expectedPoolCatchupTime(nodeCount=len(pool_of_nodes))
        looper.run(eventually(check_ledger_state, node_to_restart, DOMAIN_LEDGER_ID,
                              LedgerState.synced, retryWait=.5, timeout=timeout))
        looper.run(eventually(check_ledger_state, node_to_restart, POOL_LEDGER_ID,
                              LedgerState.synced, retryWait=.5, timeout=timeout))
        looper.run(eventually(catchuped, node_to_restart, timeout=2 * timeout))
        return pool_of_nodes

    nodes_names = sorted([n.name for n in txnPoolNodeSet], reverse=True)
    pool_of_nodes = txnPoolNodeSet
    for __ in range(3):
        node_to_restart = [n for n in pool_of_nodes if n.name == nodes_names[__]][0]
        assert not node_to_restart.has_master_primary
        pool_of_nodes = start_stop_one_node(node_to_restart, pool_of_nodes)
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_steward, 1)
        ensure_all_nodes_have_same_data(looper,
                                        pool_of_nodes,
                                        custom_timeout=tconf.VIEW_CHANGE_TIMEOUT)
