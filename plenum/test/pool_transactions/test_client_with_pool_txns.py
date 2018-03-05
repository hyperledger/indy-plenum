from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.test.pool_transactions.helper import sdk_pool_refresh
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check
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
        node.stop()
        looper.removeProdable(node)

    txnPoolNodeSet = []
    for nm in poolTxnNodeNames:
        config_helper = PNodeConfigHelper(nm, tconf, chroot=tdir)
        node = TestNode(nm,
                        config_helper=config_helper,
                        config=tconf, pluginPaths=allPluginsPath)
        looper.add(node)
        txnPoolNodeSet.append(node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    def chk():
        for node in txnPoolNodeSet:
            assert node.isParticipating

    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))

    sdk_pool_refresh(looper, sdk_pool_handle)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_new_client, sdk_pool_handle)
