from plenum.common.log import getlogger
from plenum.common.port_dispenser import genHa
from plenum.test.eventually import eventually
from plenum.test.helper import TestNode, checkNodesConnected
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import changeNodeHa, \
    buildPoolClientAndWallet

logger = getlogger()


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
    looper.removeProdable(newNode)

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
    looper.run(eventually(checkNodeLedgersForEquality, node,
                          *restartedNodes[:-1], retryWait=1, timeout=10))

    # Building a new client that reads from the genesis txn file
    # but is able to connect to all nodes
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithPoolTxns)
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)
