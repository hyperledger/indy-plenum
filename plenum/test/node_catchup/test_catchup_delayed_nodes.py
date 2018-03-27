import pytest

from stp_core.common.log import getlogger

from plenum.test import waits
from plenum.test.delayers import cpDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, \
    sdk_pool_refresh
from plenum.test.test_node import checkNodesConnected

logger = getlogger()

txnCount = 10

whitelist = ['found legacy entry']  # logged errors to ignore


@pytest.mark.skip(reason="SOV-551. Incomplete implementation")
def testCatchupDelayedNodes(txnPoolNodeSet,
                            sdk_node_set_with_node_added_after_some_txns,
                            sdk_wallet_steward,
                            txnPoolCliNodeReg, tdirWithPoolTxns,
                            tconf, tdir,
                            allPluginsPath):
    """
    Node sends catchup request to other nodes for only those sequence numbers
    that other nodes have. Have pool of connected nodes with some transactions
    made and then two more nodes say X and Y will join where Y node will start
    its catchup process after some time. The node starting late, i.e. Y should
    not receive any catchup requests
    :return:
    """
    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_set_with_node_added_after_some_txns
    stewardXName = "testClientStewardX"
    nodeXName = "Zeta"
    stewardYName = "testClientStewardY"
    nodeYName = "Eta"
    stewardZName = "testClientStewardZ"
    nodeZName = "Theta"
    delayX = 45
    delayY = 2
    stewardX, nodeX = sdk_add_new_steward_and_node(looper,
                                                   sdk_pool_handle,
                                                   sdk_wallet_steward,
                                                   stewardXName,
                                                   nodeXName,
                                                   tdir,
                                                   tconf,
                                                   autoStart=False,
                                                   allPluginsPath=allPluginsPath)

    stewardY, nodeY = sdk_add_new_steward_and_node(looper,
                                                   sdk_pool_handle,
                                                   sdk_wallet_steward,
                                                   stewardYName,
                                                   nodeYName,
                                                   tdir,
                                                   tconf,
                                                   autoStart=False,
                                                   allPluginsPath=allPluginsPath)
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
    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 50)
    logger.debug("Starting the 2 stopped nodes, {} and {}".format(nodeX.name,
                                                                  nodeY.name))
    nodeX.start(looper.loop)
    nodeY.start(looper.loop)
    waitNodeDataEquality(looper, nodeX, *txnPoolNodeSet[:5])
    waitNodeDataEquality(looper, nodeY, *txnPoolNodeSet[:5])
