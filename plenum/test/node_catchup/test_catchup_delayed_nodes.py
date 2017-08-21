import pytest

from stp_core.common.log import getlogger

from plenum.test import waits
from plenum.test.delayers import cpDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import checkNodesConnected

logger = getlogger()

txnCount = 10

whitelist = ['found legacy entry']  # logged errors to ignore


@pytest.mark.skip(reason="SOV-551. Incomplete implementation")
def testCatchupDelayedNodes(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns,
                            txnPoolCliNodeReg, tdirWithPoolTxns, tconf,
                            allPluginsPath):
    """
    Node sends catchup request to other nodes for only those sequence numbers
    that other nodes have. Have pool of connected nodes with some transactions
    made and then two more nodes say X and Y will join where Y node will start
    its catchup process after some time. The node starting late, i.e. Y should
    not receive any catchup requests
    :return:
    """
    looper, _, _, _, client, wallet = nodeSetWithNodeAddedAfterSomeTxns
    stewardXName = "testClientStewardX"
    nodeXName = "Zeta"
    stewardYName = "testClientStewardY"
    nodeYName = "Eta"
    stewardZName = "testClientStewardZ"
    nodeZName = "Theta"
    delayX = 45
    delayY = 2
    stewardX, nodeX = addNewStewardAndNode(looper, client, stewardXName,
                                           nodeXName,
                                           tdirWithPoolTxns, tconf,
                                           allPluginsPath, autoStart=False)
    stewardY, nodeY = addNewStewardAndNode(looper, client, stewardYName,
                                           nodeYName,
                                           tdirWithPoolTxns, tconf,
                                           allPluginsPath, autoStart=False)
    nodeX.nodeIbStasher.delay(cpDelay(delayX))
    nodeY.nodeIbStasher.delay(cpDelay(delayY))
    looper.add(nodeX)
    looper.add(nodeY)
    txnPoolNodeSet.append(nodeX)
    txnPoolNodeSet.append(nodeY)

    timeout = waits.expectedPoolCatchupTime(
        len(txnPoolNodeSet)) + delayX + delayY
    looper.run(checkNodesConnected(txnPoolNodeSet, customTimeout=timeout))
    logger.debug("Stopping 2 newest nodes, {} and {}".format(nodeX.name,
                                                             nodeY.name))
    nodeX.stop()
    nodeY.stop()
    logger.debug("Sending requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 50)
    logger.debug("Starting the 2 stopped nodes, {} and {}".format(nodeX.name,
                                                                  nodeY.name))
    nodeX.start(looper.loop)
    nodeY.start(looper.loop)
    waitNodeDataEquality(looper, nodeX, *txnPoolNodeSet[:5])
    waitNodeDataEquality(looper, nodeY, *txnPoolNodeSet[:5])
