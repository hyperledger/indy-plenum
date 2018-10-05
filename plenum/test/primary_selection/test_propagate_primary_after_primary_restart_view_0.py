from plenum.test.delayers import icDelay
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.helper import checkViewNoForNodes
from plenum.test.view_change.helper import start_stopped_node
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import checkNodesConnected, get_master_primary_node, ensureElectionsDone

from stp_core.common.log import getlogger

logger = getlogger()


def delay_instance_change(txnPoolNodeSet, val):
    for n in txnPoolNodeSet:
        n.nodeIbStasher.delay(icDelay(val))


def _get_ppseqno(nodes):
    res = set()
    for node in nodes:
        for repl in node.replicas.values():
            if repl.isMaster:
                res.add(repl.lastPrePrepareSeqNo)
    assert (len(res) == 1)
    return min(res)


IC_DELAY_SEC = 100


def test_propagate_primary_after_primary_restart_view_0(
        looper, txnPoolNodeSet, tconf, sdk_pool_handle, sdk_wallet_steward, tdir, allPluginsPath):
    """
    Delay instance change msgs to prevent view change during primary restart
    to test propagate primary for primary node.
    ppSeqNo should be > 0 to be able to check that propagate primary restores all
    indices correctly
    case viewNo == 0
    """
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)

    old_ppseqno = _get_ppseqno(txnPoolNodeSet)
    assert (old_ppseqno > 0)

    old_viewNo = checkViewNoForNodes(txnPoolNodeSet)
    old_primary = get_master_primary_node(txnPoolNodeSet)

    delay_instance_change(txnPoolNodeSet, IC_DELAY_SEC)

    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, old_primary, stopNode=True)

    looper.removeProdable(old_primary)

    logger.info("Restart node {}".format(old_primary))

    restartedNode = start_stopped_node(old_primary, looper, tconf, tdir, allPluginsPath,
                                       delay_instance_change_msgs=False)
    idx = [i for i, n in enumerate(txnPoolNodeSet) if n.name == restartedNode.name][0]
    txnPoolNodeSet[idx] = restartedNode

    restartedNode.nodeIbStasher.delay(icDelay(IC_DELAY_SEC))

    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    new_viewNo = checkViewNoForNodes(txnPoolNodeSet)
    assert (new_viewNo == old_viewNo)

    new_primary = get_master_primary_node(txnPoolNodeSet)
    assert (new_primary.name == old_primary.name)

    # check ppSeqNo the same
    _get_ppseqno(txnPoolNodeSet)

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)

    new_ppseqno = _get_ppseqno(txnPoolNodeSet)
    assert (new_ppseqno > old_ppseqno)
