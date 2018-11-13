from stp_core.loop.eventually import eventually

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.constants import LAST_SENT_PRE_PREPARE

from plenum.test.test_node import TestNode, ensureElectionsDone, checkNodesConnected
from plenum.test.helper import checkViewNoForNodes, sdk_send_batches_of_random, waitForViewChange
from plenum.test.restart.helper import restart_nodes

nodeCount = 7
batches_count = 3


def get_primary_replicas(nodes):
    primary_replicas = []
    for instId in nodes[0].replicas.keys():
        for idx, node in enumerate(nodes):
            if node.replicas[instId].isPrimary:
                assert instId == len(primary_replicas)
                if node.replicas[instId].isMaster:
                    primary_replicas.insert(0, node.replicas[instId])
                else:
                    primary_replicas.append(node.replicas[instId])
                break
    return primary_replicas


def all_replicas_ordered(replicas, seq_nos):
    assert len(replicas) == len(seq_nos)
    for i in range(len(replicas)):
        assert seq_nos[i] == \
               replicas[i].lastPrePrepareSeqNo


def test_persist_last_pp(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf):
    primary_replicas = get_primary_replicas(txnPoolNodeSet)

    seq_nos = list()
    for replica in primary_replicas:
        seq_nos.append(replica.lastPrePrepareSeqNo)

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               batches_count, batches_count,
                               timeout=tconf.Max3PCBatchWait)

    seq_nos = [seq_no + batches_count for seq_no in seq_nos]
    looper.run(eventually(all_replicas_ordered, primary_replicas, seq_nos))


def test_restore_persisted_last_pp_after_restart(looper, txnPoolNodeSet, tconf, tdir,
                                                 sdk_pool_handle, sdk_wallet_client,
                                                 allPluginsPath):
    primary_replicas = get_primary_replicas(txnPoolNodeSet)

    seq_nos = list()
    for replica in primary_replicas:
        seq_nos.append(replica.lastPrePrepareSeqNo)

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               batches_count, batches_count,
                               timeout=tconf.Max3PCBatchWait)

    seq_nos = [seq_no + batches_count for seq_no in seq_nos]
    looper.run(eventually(all_replicas_ordered, primary_replicas, seq_nos))

    seq_no_before_1 = primary_replicas[1].lastPrePrepareSeqNo
    seq_no_before_2 = primary_replicas[2].lastPrePrepareSeqNo

    # Restart backup primaries
    restart_nodes(looper, txnPoolNodeSet, [primary_replicas[1].node, primary_replicas[2].node],
                  tconf, tdir, allPluginsPath)
    primary_replicas_new = get_primary_replicas(txnPoolNodeSet)
    assert primary_replicas_new[0] == primary_replicas[0]

    # Check that we've persisted last send pre-prepare on backup primary replicas
    assert seq_no_before_1 == \
           primary_replicas_new[1].lastPrePrepareSeqNo
    assert seq_no_before_2 == \
           primary_replicas_new[2].lastPrePrepareSeqNo


def test_clear_persisted_last_pp_after_view_change(looper, txnPoolNodeSet, tconf, tdir,
                                                   sdk_pool_handle, sdk_wallet_client, allPluginsPath):
    primary_replicas = get_primary_replicas(txnPoolNodeSet)
    seq_nos = list()
    for replica in primary_replicas:
        seq_nos.append(replica.lastPrePrepareSeqNo)

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               batches_count, batches_count,
                               timeout=tconf.Max3PCBatchWait)

    seq_nos = [seq_no + batches_count for seq_no in seq_nos]
    looper.run(eventually(all_replicas_ordered, primary_replicas, seq_nos))
    old_view_no = primary_replicas[0].viewNo

    # Restart master primary to make a view_change
    restart_nodes(looper, txnPoolNodeSet, [primary_replicas[0].node], tconf, tdir, allPluginsPath,
                  after_restart_timeout=tconf.ToleratePrimaryDisconnection + 1)
    waitForViewChange(looper, txnPoolNodeSet, old_view_no + 1)
    ensureElectionsDone(looper, txnPoolNodeSet)

    # Check that we've cleared last send pre-prepare on every primary replica
    assert all(LAST_SENT_PRE_PREPARE not in n.nodeStatusDB for n in txnPoolNodeSet)


def test_clear_persisted_last_pp_after_pool_restart(looper, txnPoolNodeSet, tconf, tdir,
                                                    sdk_pool_handle, sdk_wallet_client):
    primary_replicas = get_primary_replicas(txnPoolNodeSet)
    seq_nos = list()
    for replica in primary_replicas:
        seq_nos.append(replica.lastPrePrepareSeqNo)

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               batches_count, batches_count,
                               timeout=tconf.Max3PCBatchWait)

    seq_nos = [seq_no + batches_count for seq_no in seq_nos]
    looper.run(eventually(all_replicas_ordered, primary_replicas, seq_nos))

    for node in txnPoolNodeSet:
        node.stop()
        looper.removeProdable(node)

    # Starting nodes again by creating `Node` objects since that simulates
    # what happens when starting the node with script
    restartedNodes = []
    for node in txnPoolNodeSet:
        config_helper = PNodeConfigHelper(node.name, tconf, chroot=tdir)
        restartedNode = TestNode(node.name,
                                 config_helper=config_helper,
                                 config=tconf, ha=node.nodestack.ha,
                                 cliha=node.clientstack.ha)
        looper.add(restartedNode)
        restartedNodes.append(restartedNode)

    looper.run(checkNodesConnected(restartedNodes))
    ensureElectionsDone(looper, restartedNodes)
    checkViewNoForNodes(restartedNodes, 0)

    # Check that we've cleared last send pre-prepare on every primary replica
    primary_replicas = get_primary_replicas(restartedNodes)
    assert primary_replicas[0].lastPrePrepareSeqNo == 0
    assert all(LAST_SENT_PRE_PREPARE not in r.node.nodeStatusDB for r in primary_replicas)
