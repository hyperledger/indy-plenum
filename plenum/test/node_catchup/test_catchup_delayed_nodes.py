from plenum.client.signer import SimpleSigner
from plenum.common.util import randomString
from plenum.test.eventually import eventually
from plenum.test.helper import cpDelay, TestClient, genHa, checkNodesConnected
from plenum.test.pool_transactions.helper import addNewStewardAndNode

txnCount = 10


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
    looper, _, _, client = nodeSetWithNodeAddedAfterSomeTxns
    stewardXName = "testClientStewardX"
    nodeXName = "X"
    stewardYName = "testClientStewardY"
    nodeYName = "Y"
    stewardX, nodeX = addNewStewardAndNode(looper, client, stewardXName,
                                               nodeXName, txnPoolCliNodeReg,
                                               tdirWithPoolTxns, tconf,
                                               allPluginsPath)
    stewardY, nodeY = addNewStewardAndNode(looper, client, stewardYName,
                                           nodeYName, txnPoolCliNodeReg,
                                           tdirWithPoolTxns, tconf,
                                           allPluginsPath)
    nodeX.nodeIbStasher.delay(cpDelay(5))
    nodeY.nodeIbStasher.delay(cpDelay(5))
    txnPoolNodeSet.append(nodeX)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=7))
    txnPoolNodeSet.append(nodeY)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=10))
