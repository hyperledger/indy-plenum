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
    tconf.RETRY_TIMEOUT_RESTRICTED = 2
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = 2
    yield tconf

    tconf.RETRY_TIMEOUT_RESTRICTED = old_timeout_restricted
    tconf.RETRY_TIMEOUT_NOT_RESTRICTED = old_timeout_not_restricted


def check_count_connected_node(nodes, expected_count):
    assert set([n.connectedNodeCount for n in nodes]) == {expected_count}


def test_reconnect_primary_and_not_primary(looper,
                                        txnPoolNodeSet,
                                        sdk_wallet_steward,
                                        sdk_pool_handle,
                                        tconf):
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
    # Disconnect node after all primaries (after all backup primaries)
    disconnect_node_and_ensure_disconnected(looper,
                                            restNodes,
                                            node_after_all_primary,
                                            stopNode=False)
    # -------------------------------------------------------
    restNodes.remove(node_after_all_primary)
    looper.run(eventually(partial(check_count_connected_node, restNodes, 6),
                          timeout=5,
                          acceptableExceptions=[AssertionError]))
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)
    # Get primary node for backup replica
    primary_node = txnPoolNodeSet[0]
    assert primary_node.master_replica.isPrimary
    old_view_no = checkViewNoForNodes(restNodes, 0)
    # disconnect primary node
    disconnect_node_and_ensure_disconnected(looper,
                                            restNodes,
                                            primary_node,
                                            stopNode=False)
    # -------------------------------------------------------
    restNodes.remove(primary_node)
    looper.run(eventually(partial(check_count_connected_node, restNodes, 5),
                          timeout=5,
                          acceptableExceptions=[AssertionError]))
    looper.run(eventually(partial(checkViewNoForNodes, restNodes, expectedViewNo=old_view_no + 1),
                          timeout=tconf.VIEW_CHANGE_TIMEOUT))
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)
    logger.debug("restNodes: {}".format(restNodes))
    restNodes.add(node_after_all_primary)
    # Return back node after all primary
    reconnect_node_and_ensure_connected(looper, restNodes, node_after_all_primary)
    looper.run(checkNodesConnected(restNodes,
                                   customTimeout=5*tconf.RETRY_TIMEOUT_RESTRICTED))
    looper.run(eventually(partial(check_count_connected_node, restNodes, 6),
                          timeout=5,
                          acceptableExceptions=[AssertionError]))
    assert len(set([len(n.replicas) for n in restNodes])) == 1
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)
    # Return back primary node
    restNodes.add(primary_node)
    reconnect_node_and_ensure_connected(looper, restNodes, primary_node)
    looper.run(checkNodesConnected(restNodes,
                                   customTimeout=5*tconf.RETRY_TIMEOUT_RESTRICTED))
    sdk_send_random_and_check(looper, restNodes, sdk_pool_handle, sdk_wallet_steward, 5)


