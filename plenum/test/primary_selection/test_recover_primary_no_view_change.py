import pytest

from stp_core.common.log import getlogger

from plenum.test.conftest import getValueFromModule
from plenum.test.helper import stopNodes, waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected
from plenum.test.test_node import ensureElectionsDone, ensure_node_disconnected
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.view_change.helper import start_stopped_node

from plenum.test.primary_selection.test_recover_more_than_f_failure import \
    stop_primary, checkpoint_size, primary_replicas_iter, nodes_have_checkpoints, nodes_do_not_have_checkpoints

# Do not remove these imports
from plenum.test.pool_transactions.conftest import client1, wallet1, client1Connected, looper


logger = getlogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    old_val = tconf.ToleratePrimaryDisconnection
    tconf.ToleratePrimaryDisconnection = 1000
    yield tconf
    tconf.ToleratePrimaryDisconnection = old_val


def test_recover_stop_primaries_no_view_change(looper, checkpoint_size, txnPoolNodeSet,
                                               allPluginsPath, tdir, tconf, client1, wallet1,
                                               client1Connected):
    """
    Test that we can recover after having more than f nodes disconnected:
    - send txns
    - stop current master primary
    - restart current master primary
    - send txns
    """

    active_nodes = list(txnPoolNodeSet)
    assert 4 == len(active_nodes)
    initial_view_no = active_nodes[0].viewNo

    logger.info("send at least one checkpoint")
    assert nodes_do_not_have_checkpoints(*active_nodes)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, numReqs=2*checkpoint_size)
    assert nodes_have_checkpoints(*active_nodes)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    logger.info("Stop first node (current Primary)")
    stopped_node, active_nodes = stop_primary(looper, active_nodes)

    logger.info("Restart the primary node")
    restarted_node = start_stopped_node(stopped_node, looper, tconf, tdir, allPluginsPath)
    assert nodes_do_not_have_checkpoints(restarted_node)
    assert nodes_have_checkpoints(*active_nodes)
    active_nodes = active_nodes + [restarted_node]

    logger.info("Check that primary selected")
    ensureElectionsDone(looper=looper, nodes=active_nodes,
                        numInstances=2, customTimeout=30)
    waitForViewChange(looper, active_nodes, expectedViewNo=0)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)

    logger.info("Check if the pool is able to process requests")
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, numReqs=10*checkpoint_size)
    ensure_all_nodes_have_same_data(looper, nodes=active_nodes)
    assert nodes_have_checkpoints(*active_nodes)
