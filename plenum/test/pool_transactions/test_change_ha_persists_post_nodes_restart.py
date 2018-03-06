from plenum.common.util import hexToFriendly, randomString
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_send_update_node, sdk_pool_refresh, \
    sdk_add_new_steward_and_node
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.pool_transactions.conftest import looper

logger = getlogger()


def testChangeHaPersistsPostNodesRestart(looper, txnPoolNodeSet,
                                         tdir, tconf,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         sdk_wallet_steward):
    new_steward_wallet, new_node = \
        sdk_add_new_steward_and_node(looper,
                                     sdk_pool_handle,
                                     sdk_wallet_steward,
                                     'AnotherSteward' + randomString(4),
                                     'AnotherNode' + randomString(4),
                                     tdir,
                                     tconf)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)

    node_new_ha, client_new_ha = genHa(2)
    logger.debug("{} changing HAs to {} {}".format(new_node, node_new_ha,
                                                   client_new_ha))

    # Making the change HA txn an confirming its succeeded
    node_dest = hexToFriendly(new_node.nodestack.verhex)
    sdk_send_update_node(looper, new_steward_wallet, sdk_pool_handle,
                         node_dest, new_node.name,
                         node_new_ha.host, node_new_ha.port,
                         client_new_ha.host, client_new_ha.port)

    # Stopping existing nodes
    for node in txnPoolNodeSet:
        node.stop()
        looper.removeProdable(node)

    # Starting nodes again by creating `Node` objects since that simulates
    # what happens when starting the node with script
    restartedNodes = []
    for node in txnPoolNodeSet[:-1]:
        config_helper = PNodeConfigHelper(node.name, tconf, chroot=tdir)
        restartedNode = TestNode(node.name,
                                 config_helper=config_helper,
                                 config=tconf, ha=node.nodestack.ha,
                                 cliha=node.clientstack.ha)
        looper.add(restartedNode)
        restartedNodes.append(restartedNode)

    # Starting the node whose HA was changed
    config_helper = PNodeConfigHelper(new_node.name, tconf, chroot=tdir)
    node = TestNode(new_node.name,
                    config_helper=config_helper,
                    config=tconf,
                    ha=node_new_ha, cliha=client_new_ha)
    looper.add(node)
    restartedNodes.append(node)

    looper.run(checkNodesConnected(restartedNodes))
    waitNodeDataEquality(looper, node, *restartedNodes[:-1])
    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_ensure_pool_functional(looper, restartedNodes, sdk_wallet_client, sdk_pool_handle)
