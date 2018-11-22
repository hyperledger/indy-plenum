from plenum.common.constants import LAST_SENT_PRE_PREPARE
from plenum.test import waits
from plenum.test.checkpoints.conftest import chkFreqPatched
from plenum.test.helper import sdk_send_batches_of_random, assertExp
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getPrimaryReplica, \
    checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually

CHK_FREQ = 2
LOG_SIZE = 3 * CHK_FREQ

nodeCount = 4

backup_inst_id = 1


def test_node_erases_last_sent_pp_key_on_propagate_primary_after_pool_restart(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
        tconf, tdir, allPluginsPath, chkFreqPatched):

    # Get a node with a backup primary replica and the rest of the nodes
    replica = getPrimaryReplica(txnPoolNodeSet, instId=backup_inst_id)
    node = replica.node
    other_nodes = set(txnPoolNodeSet) - {node}

    # Send some 3PC-batches and wait until the replica orders the 3PC-batches
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=7, num_batches=7,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(replica.last_ordered_3pc == (0, 7)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))

    # Check view no of the node and lastPrePrepareSeqNo of the replica
    assert node.viewNo == 0
    assert replica.lastPrePrepareSeqNo == 7
    assert replica.h == 6
    assert replica.H == 6 + LOG_SIZE

    # Ensure that there is a stored last sent PrePrepare key on the node
    assert LAST_SENT_PRE_PREPARE in node.nodeStatusDB

    # Stop the node containing the replica
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node.name,
                                            stopNode=True)
    looper.removeProdable(node)
    txnPoolNodeSet.remove(node)

    # Restart the rest of the nodes and wait for primary elections done
    for n in other_nodes:
        disconnect_node_and_ensure_disconnected(looper,
                                                txnPoolNodeSet,
                                                n.name,
                                                timeout=nodeCount,
                                                stopNode=True)
        looper.removeProdable(n)
        txnPoolNodeSet.remove(n)
    for n in other_nodes:
        txnPoolNodeSet.append(start_stopped_node(n, looper, tconf, tdir, allPluginsPath))
    looper.run(checkNodesConnected(txnPoolNodeSet,
                                   customTimeout=waits.expectedPoolInterconnectionTime(nodeCount)))
    def chk():
        for n in txnPoolNodeSet:
            for r in n.replicas.values():
                assert r.hasPrimary
    looper.run(eventually(chk, retryWait=1, timeout=waits.expectedPoolElectionTimeout(nodeCount)))

    # Start the stopped node
    node = start_stopped_node(node, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(node)

    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper, txnPoolNodeSet)

    replica = node.replicas[backup_inst_id]

    # Verify that the node has erased the stored last sent PrePrepare key
    assert LAST_SENT_PRE_PREPARE not in node.nodeStatusDB

    # Verify correspondingly that after the propagate primary procedure
    # the replica (which must again be the primary in its instance) has
    # not restored lastPrePrepareSeqNo, not adjusted last_ordered_3pc and
    # not shifted the watermarks
    assert node.viewNo == 0
    assert replica.isPrimary
    assert replica.lastPrePrepareSeqNo == 0
    assert replica.last_ordered_3pc == (0, 0)
    assert replica.h == 0
    assert replica.H == 0 + LOG_SIZE

    # Send a 3PC-batch and ensure that the replica orders it
    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               num_reqs=1, num_batches=1,
                               timeout=tconf.Max3PCBatchWait)

    looper.run(
        eventually(lambda: assertExp(replica.last_ordered_3pc == (0, 1)),
                   retryWait=1,
                   timeout=waits.expectedTransactionExecutionTime(nodeCount)))
