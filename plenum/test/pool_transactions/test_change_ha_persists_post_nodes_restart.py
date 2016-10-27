from plenum.common.log import getlogger
from plenum.common.port_dispenser import genHa
from plenum.test.eventually import eventually
from plenum.test.helper import TestNode, checkNodesConnected
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.pool_transactions.helper import changeNodeHa

logger = getlogger()


def testChangeHaPersistsPostNodesRestart(looper, txnPoolNodeSet,
                                         tdirWithPoolTxns, tconf, steward1,
                                         stewardWallet, nodeThetaAdded):
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    nodeNewHa, clientNewHa = genHa(2)
    logger.debug("{} changing HAs to {} {}".format(newNode, nodeNewHa,
                                                   clientNewHa))
    changeNodeHa(looper, newSteward, newStewardWallet, newNode,
                 nodeHa=nodeNewHa, clientHa=clientNewHa)
    for node in txnPoolNodeSet:
        node.stop()
    looper.removeProdable(newNode)
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
