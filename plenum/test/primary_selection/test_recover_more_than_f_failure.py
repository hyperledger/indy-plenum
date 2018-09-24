from stp_core.common.log import getlogger

from plenum.test.helper import waitForViewChange, \
    sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import start_stopped_node

logger = getlogger()


def test_recover_stop_primaries(looper, checkpoint_size, txnPoolNodeSet,
                                allPluginsPath, tdir, tconf, sdk_pool_handle,
                                sdk_wallet_steward):
    """
    Test that we can recover after having more than f nodes disconnected:
    - stop current master primary (Alpha)
    - send txns
    - restart current master primary (Beta)
    - send txns
    """

    active_nodes = list(txnPoolNodeSet)
    assert 4 == len(active_nodes)
    initial_view_no = active_nodes[0].viewNo

    logger.info("Stop first node (current Primary)")
    _, active_nodes = stop_primary(looper, active_nodes)

    logger.info("Make sure view changed")
    expected_view_no = initial_view_no + 1
    waitForViewChange(looper, active_nodes, expectedViewNo=expected_view_no)
    ensureElectionsDone(looper=looper, nodes=active_nodes, numInstances=2)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    logger.info("send at least one checkpoint")
    assert nodes_do_not_have_checkpoints(*active_nodes)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 2 * checkpoint_size)
    assert nodes_have_checkpoints(*active_nodes)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    logger.info("Stop second node (current Primary) so the primary looses his state")
    stopped_node, active_nodes = stop_primary(looper, active_nodes)

    logger.info("Restart the primary node")
    restarted_node = start_stopped_node(stopped_node, looper, tconf, tdir, allPluginsPath)
    assert nodes_do_not_have_checkpoints(restarted_node)
    assert nodes_have_checkpoints(*active_nodes)
    active_nodes = active_nodes + [restarted_node]

    logger.info("Check that primary selected")
    ensureElectionsDone(looper=looper, nodes=active_nodes,
                        numInstances=2, customTimeout=30)
    waitForViewChange(looper, active_nodes, expectedViewNo=expected_view_no)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    logger.info("Check if the pool is able to process requests")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, 10 * checkpoint_size)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)
    assert nodes_have_checkpoints(*active_nodes)


def stop_primary(looper, active_nodes):
    stopped_node = active_nodes[0]
    disconnect_node_and_ensure_disconnected(looper,
                                            active_nodes,
                                            stopped_node,
                                            stopNode=True)
    looper.removeProdable(stopped_node)
    active_nodes = active_nodes[1:]
    return stopped_node, active_nodes


def primary_replicas_iter(*nodes):
    for node in nodes:
        for replica in node.replicas.values():
            if replica.isPrimary:
                yield replica


def nodes_have_checkpoints(*nodes):
    return all(replica.checkpoints for replica in primary_replicas_iter(*nodes))


def nodes_do_not_have_checkpoints(*nodes):
    return all(not replica.checkpoints for replica in primary_replicas_iter(*nodes))
