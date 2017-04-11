from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test.node_catchup.helper import waitNodeLedgersEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import changeNodeHa, \
    buildPoolClientAndWallet
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.network.port_dispenser import genHa

logger = getlogger()


whitelist = ['found legacy entry', "doesn't match", "reconciling nodeReg",
             "missing", "conflicts", "matches", "nodeReg",
             "conflicting address", "got error while verifying message"]


def testChangeHaPersistsPostNodesRestart(looper, txnPoolNodeSet,
                                         tdirWithPoolTxns, tconf, steward1,
                                         stewardWallet, nodeThetaAdded,
                                         poolTxnClientData):
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    nodeNewHa, clientNewHa = genHa(2)
    logger.debug("{} changing HAs to {} {}".format(newNode, nodeNewHa,
                                                   clientNewHa))

    # Making the change HA txn an confirming its succeeded
    changeNodeHa(looper, newSteward, newStewardWallet, newNode,
                nodeHa=nodeNewHa, clientHa=clientNewHa)

    # Stopping existing nodes
    for node in txnPoolNodeSet:
        node.stop()
        looper.removeProdable(node)

    # Starting nodes again by creating `Node` objects since that simulates
    # what happens when starting the node with script
    restartedNodes = []
    for node in txnPoolNodeSet[:-1]:
        restartedNode = TestNode(node.name, basedirpath=tdirWithPoolTxns,
                                 config=tconf, ha=node.nodestack.ha,
                                 cliha=node.clientstack.ha)
        looper.add(restartedNode)
        restartedNodes.append(restartedNode)

    # Starting the node whose HA was changed
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeNewHa, cliha=clientNewHa)
    looper.add(node)
    restartedNodes.append(node)

    looper.run(checkNodesConnected(restartedNodes))
    waitNodeLedgersEquality(looper, node, *restartedNodes[:-1])

    # Building a new client that reads from the genesis txn file
    # but is able to connect to all nodes
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithPoolTxns)
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *restartedNodes)
