import pytest

from stp_core.loop.eventually import eventually
from plenum.common.log import getlogger
from plenum.test.delayers import cpDelay
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
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
    stewardX, nodeX = addNewStewardAndNode(looper, client, stewardXName,
                                               nodeXName,
                                               tdirWithPoolTxns, tconf,
                                               allPluginsPath, autoStart=False)
    stewardY, nodeY = addNewStewardAndNode(looper, client, stewardYName,
                                           nodeYName,
                                           tdirWithPoolTxns, tconf,
                                           allPluginsPath, autoStart=False)
    nodeX.nodeIbStasher.delay(cpDelay(45))
    nodeY.nodeIbStasher.delay(cpDelay(2))
    looper.add(nodeX)
    looper.add(nodeY)
    txnPoolNodeSet.append(nodeX)
    txnPoolNodeSet.append(nodeY)

    looper.run(checkNodesConnected(txnPoolNodeSet, overrideTimeout=60))
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
    looper.run(eventually(checkNodeLedgersForEquality, nodeX,
                          *txnPoolNodeSet[:5], retryWait=1, timeout=15))
    looper.run(eventually(checkNodeLedgersForEquality, nodeY,
                          *txnPoolNodeSet[:5], retryWait=1, timeout=15))
