import pytest
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.test_node import checkNodesConnected
from stp_core.loop.eventually import eventually
from functools import partial
from plenum.test.pool_transactions.helper import reconnect_node_and_ensure_connected
from stp_core.common.log import getlogger


logger = getlogger()

nodeCount = 7

@pytest.fixture(scope="module")
def tconf(tconf):
    old_timeout_restricted = tconf.RETRY_TIMEOUT_RESTRICTED
    old_timeout_not_restricted = tconf.RETRY_TIMEOUT_NOT_RESTRICTED
    tconf.RETRY_TIMEOUT_RESTRICTED = 10
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = 10
    yield tconf

    tconf.RETRY_TIMEOUT_RESTRICTED = old_timeout_restricted
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = old_timeout_not_restricted


def test_reboot_primary_and_not_primary(looper,
                                        txnPoolNodeSet,
                                        sdk_wallet_steward,
                                        sdk_pool_handle,
                                        tconf,
                                        tdir,
                                        allPluginsPath):
    """
    Test steps:
    Pool of 7 nodes.
    count of instances must be 3
    1. Choose node, that is not primary on all replicas (3 index)
    2. Disconnect them
    3. Ensure, that number of replicas was decreased
    4. Choose current primary node (must be 0)
    5. Disconnect primary
    6. Ensure, that view change complete and primary was selected
    7. Add node back from 1 step
    8. Add node back from 4 step
    9. Check, that count of instance (f+1 = 3)
    10. Send some requests and check, that pool works.
    """
    restNodes = set(txnPoolNodeSet)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 5)
    assert txnPoolNodeSet[0].master_replica.isPrimary
    node_after_all_primary = txnPoolNodeSet[3]
    logger.debug("Node after all primaries: {}".format(node_after_all_primary))
    disconnect_node_and_ensure_disconnected(looper,
                                            restNodes,
                                            node_after_all_primary,
                                            stopNode=False)
    # -------------------------------------------------------
    restNodes.remove(node_after_all_primary)
    assert set([n.connectedNodeCount for n in restNodes]) == {6}
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)
    primary_node = txnPoolNodeSet[0]
    assert primary_node.master_replica.isPrimary
    old_view_no = checkViewNoForNodes(restNodes, 0)
    disconnect_node_and_ensure_disconnected(looper,
                                            restNodes,
                                            primary_node,
                                            stopNode=False)
    # -------------------------------------------------------
    restNodes.remove(primary_node)
    assert set([n.connectedNodeCount for n in restNodes]) == {5}
    assert set([len(n.replicas) for n in restNodes]) == {2}
    looper.run(eventually(partial(checkViewNoForNodes, restNodes, expectedViewNo=old_view_no + 1),
                          timeout=tconf.VIEW_CHANGE_TIMEOUT))
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)
    logger.debug("restNodes: {}".format(restNodes))
    restNodes.add(node_after_all_primary)
    reconnect_node_and_ensure_connected(looper, restNodes, node_after_all_primary)
    looper.run(checkNodesConnected(restNodes,
                                   customTimeout=5*tconf.RETRY_TIMEOUT_RESTRICTED))
    assert len(set([len(n.replicas) for n in restNodes])) == 1
    # assert restNodes[-1].requiredNumberOfInstances == 2
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)
    restNodes.add(primary_node)
    reconnect_node_and_ensure_connected(looper, restNodes, primary_node)
    looper.run(checkNodesConnected(restNodes,
                                   customTimeout=5*tconf.RETRY_TIMEOUT_RESTRICTED))
    assert len(set([len(n.replicas) for n in restNodes])) == 1
    assert set([len(n.replicas) for n in restNodes]) == {3}
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)


