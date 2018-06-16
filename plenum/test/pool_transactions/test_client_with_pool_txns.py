from plenum.test.pool_transactions.helper import sdk_pool_refresh, disconnect_node_and_ensure_disconnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.test_node import checkNodesConnected, TestNode, \
    ensureElectionsDone
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()


def testClientConnectToRestartedNodes(looper, txnPoolNodeSet,
                                      tdir, tconf,
                                      poolTxnNodeNames, allPluginsPath,
                                      sdk_wallet_new_client,
                                      sdk_pool_handle):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_new_client, 1)
    for node in txnPoolNodeSet:
        disconnect_node_and_ensure_disconnected(looper,
                                                txnPoolNodeSet,
                                                node,
                                                stopNode=True)

    txnPoolNodeSetNew = []
    for node in txnPoolNodeSet:
        new_node = start_stopped_node(node,
                                      looper,
                                      tconf,
                                      tdir,
                                      allPluginsPath=allPluginsPath)
        txnPoolNodeSetNew.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSetNew))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSetNew)

    def chk():
        for node in txnPoolNodeSetNew:
            assert node.isParticipating

    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSetNew))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))

    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_ensure_pool_functional(looper, txnPoolNodeSetNew, sdk_wallet_new_client, sdk_pool_handle)
