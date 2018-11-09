from common.serializers.serialization import node_status_db_serializer
from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.constants import LAST_SENT_PRE_PREPARE

from plenum.test.test_node import TestNode, ensureElectionsDone, checkNodesConnected

from plenum.test.helper import sdk_send_batches_of_random_and_check, \
    checkViewNoForNodes, sdk_send_batches_of_random
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


def test_persist_last_pp(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf):
    primary_replicas = get_primary_replicas(txnPoolNodeSet)

    seq_no_before_0 = primary_replicas[0].lastPrePrepareSeqNo
    seq_no_before_1 = primary_replicas[1].lastPrePrepareSeqNo
    seq_no_before_2 = primary_replicas[2].lastPrePrepareSeqNo

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               batches_count, batches_count,
                               timeout=tconf.Max3PCBatchWait)

    # Check that we've persisted last send pre-prepare on every primary replica
    assert seq_no_before_0 + batches_count == \
           node_status_db_serializer.deserialize(
               primary_replicas[0].node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE))[2]
    assert seq_no_before_1 + batches_count == \
           node_status_db_serializer.deserialize(
               primary_replicas[1].node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE))[2]
    assert seq_no_before_2 + batches_count == \
           node_status_db_serializer.deserialize(
               primary_replicas[2].node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE))[2]


def test_restore_persisted_last_pp_after_restart(looper, txnPoolNodeSet, tconf, tdir,
                                                 sdk_pool_handle, sdk_wallet_client,
                                                 allPluginsPath):
    primary_replicas = get_primary_replicas(txnPoolNodeSet)

    sdk_send_batches_of_random(looper, txnPoolNodeSet,
                               sdk_pool_handle, sdk_wallet_client,
                               batches_count, batches_count,
                               timeout=tconf.Max3PCBatchWait)

    seq_no_before_1 = primary_replicas[1].lastPrePrepareSeqNo
    seq_no_before_2 = primary_replicas[2].lastPrePrepareSeqNo

    # Restart backup primaries
    restart_nodes(looper, txnPoolNodeSet, [primary_replicas[1].node, primary_replicas[2].node],
                  tconf, tdir, allPluginsPath)
    primary_replicas_new = get_primary_replicas(txnPoolNodeSet)
    assert primary_replicas_new[0] == primary_replicas[0]

    # Check that we've persisted last send pre-prepare on backup primary replicas
    assert seq_no_before_1 == \
           node_status_db_serializer.deserialize(
               primary_replicas_new[1].node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE))[2]
    assert seq_no_before_2 == \
           node_status_db_serializer.deserialize(
               primary_replicas_new[2].node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE))[2]


def test_clear_persisted_last_pp_after_view_change(looper, txnPoolNodeSet, tconf, tdir,
                                                   sdk_pool_handle, sdk_wallet_client, allPluginsPath):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)

    primary_replicas = get_primary_replicas(txnPoolNodeSet)
    old_3pc = primary_replicas[0].last_ordered_3pc
    # Restart master primary to make a view_change
    restart_nodes(looper, txnPoolNodeSet, [primary_replicas[0].node], tconf, tdir, allPluginsPath,
                  after_restart_timeout=tconf.ToleratePrimaryDisconnection + 1)
    primary_replicas_new = get_primary_replicas(txnPoolNodeSet)
    assert primary_replicas[0] not in primary_replicas_new

    # Check that we've cleared last send pre-prepare on every primary replica
    assert LAST_SENT_PRE_PREPARE not in primary_replicas_new[1].node.nodeStatusDB

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)
    persisted = node_status_db_serializer.deserialize(
        primary_replicas_new[1].node.nodeStatusDB.get(LAST_SENT_PRE_PREPARE))
    assert persisted[2] == batches_count
    assert persisted[1] == old_3pc[0] + 1


def test_clear_persisted_last_pp_after_pool_restart(looper, txnPoolNodeSet, tconf, tdir,
                                                    sdk_pool_handle, sdk_wallet_client):
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_client, batches_count, batches_count)

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
    assert primary_replicas[0].last_ordered_3pc == (0, 0) and \
           primary_replicas[0].lastPrePrepare is None
    assert LAST_SENT_PRE_PREPARE not in primary_replicas[0].node.nodeStatusDB
