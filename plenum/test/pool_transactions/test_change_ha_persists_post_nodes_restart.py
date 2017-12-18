from plenum.common.constants import ALIAS, NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import updateNodeData, \
    buildPoolClientAndWallet
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.network.port_dispenser import genHa
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()


whitelist = ['found legacy entry', "doesn't match", "reconciling nodeReg",
             "missing", "conflicts", "matches", "nodeReg",
             "conflicting address", "got error while verifying message"]


def testChangeHaPersistsPostNodesRestart(looper, txnPoolNodeSet, tdir, tdirWithPoolTxns,
                                         tdirWithClientPoolTxns, tconf, steward1,
                                         stewardWallet, nodeThetaAdded,
                                         poolTxnClientData):
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    nodeNewHa, clientNewHa = genHa(2)
    logger.debug("{} changing HAs to {} {}".format(newNode, nodeNewHa,
                                                   clientNewHa))

    # Making the change HA txn an confirming its succeeded
    op = {
        ALIAS: newNode.name,
        NODE_IP: nodeNewHa.host,
        NODE_PORT: nodeNewHa.port,
        CLIENT_IP: clientNewHa.host,
        CLIENT_PORT: clientNewHa.port,
    }
    updateNodeData(looper, newSteward, newStewardWallet, newNode,
                   op)

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
    config_helper = PNodeConfigHelper(newNode.name, tconf, chroot=tdir)
    node = TestNode(newNode.name,
                    config_helper=config_helper,
                    config=tconf,
                    ha=nodeNewHa, cliha=clientNewHa)
    looper.add(node)
    restartedNodes.append(node)

    looper.run(checkNodesConnected(restartedNodes))
    waitNodeDataEquality(looper, node, *restartedNodes[:-1])

    # Building a new client that reads from the genesis txn file
    # but is able to connect to all nodes
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithClientPoolTxns)
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *restartedNodes)
