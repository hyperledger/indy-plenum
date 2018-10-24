from contextlib import ExitStack
import pytest

from plenum.common.messages.node_messages import BackupInstanceFaulty
from plenum.server.suspicion_codes import Suspicions
from plenum.test.replica.helper import check_replica_removed
from stp_core.loop.eventually import eventually
from plenum.test.helper import waitForViewChange, create_new_test_node
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected


@pytest.fixture(scope="module", params=[{"instance_degraded": "local", "instance_primary_disconnected": "local"},
                                        {"instance_degraded": "local", "instance_primary_disconnected": "quorum"},
                                        {"instance_degraded": "quorum", "instance_primary_disconnected": "local"},
                                        {"instance_degraded": "quorum", "instance_primary_disconnected": "quorum"}])
def txnPoolNodeSet(node_config_helper_class,
                   patchPluginManager,
                   txnPoolNodesLooper,
                   tdirWithPoolTxns,
                   tdirWithDomainTxns,
                   tdir,
                   tconf,
                   poolTxnNodeNames,
                   allPluginsPath,
                   tdirWithNodeKeepInited,
                   testNodeClass,
                   do_post_node_creation,
                   request):
    update_conf(tconf, request.param)
    with ExitStack() as exitStack:
        nodes = []
        for nm in poolTxnNodeNames:
            node = exitStack.enter_context(create_new_test_node(
                testNodeClass, node_config_helper_class, nm, tconf, tdir,
                allPluginsPath))
            do_post_node_creation(node)
            txnPoolNodesLooper.add(node)
            nodes.append(node)
        txnPoolNodesLooper.run(checkNodesConnected(nodes))
        ensureElectionsDone(looper=txnPoolNodesLooper, nodes=nodes)
        yield nodes
        for node in nodes:
            txnPoolNodesLooper.removeProdable(node)


@pytest.fixture(scope="function")
def do_view_change(txnPoolNodeSet, looper, tconf):
    view_no = txnPoolNodeSet[0].viewNo
    # start View Change
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=view_no + 1,
                      customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # check that all replicas were restored
    assert all(node.requiredNumberOfInstances == node.replicas.num_replicas
               for node in txnPoolNodeSet)


def update_conf(tconf, tmp):
    tconf.REPLICAS_REMOVING_WITH_DEGRADATION = tmp["instance_degraded"]
    tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = tmp["instance_primary_disconnected"]
    return tconf


def test_on_backup_degradation(looper,
                               txnPoolNodeSet,
                               do_view_change):
    """
    1. Start backup degraded.
    2. Check that degraded replicas were removed.
    """
    instance_to_remove = 1
    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    for node in txnPoolNodeSet:
        node.backup_instance_faulty_processor.on_backup_degradation([instance_to_remove])
    __check_replica_removed_on_all_nodes(looper,
                                         txnPoolNodeSet,
                                         start_replicas_count,
                                         instance_to_remove)


def test_on_backup_primary_disconnected(looper,
                                        txnPoolNodeSet,
                                        do_view_change):
    """
    1. Start backup primary disconnected.
    2. Check that replicas were removed.
    """
    instance_to_remove = 1
    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    for node in txnPoolNodeSet:
        node.backup_instance_faulty_processor.on_backup_primary_disconnected([instance_to_remove])

    __check_replica_removed_on_all_nodes(looper,
                                         txnPoolNodeSet,
                                         start_replicas_count,
                                         instance_to_remove)


def test_restore_replicas(looper,
                          txnPoolNodeSet,
                          do_view_change):
    instance_to_remove = 1
    for node in txnPoolNodeSet:
        node.replicas.remove_replica(instance_to_remove)
    for node in txnPoolNodeSet:
        node.backup_instance_faulty_processor.restore_replicas()
    # check that all replicas were restored
    assert all(node.requiredNumberOfInstances == node.replicas.num_replicas
               for node in txnPoolNodeSet)


def test_process_backup_instance_faulty_msg(looper,
                                            txnPoolNodeSet,
                                            tconf):
    __process_backup_instance_faulty_msg_work_with_different_msgs(looper,
                                                                  txnPoolNodeSet,
                                                                  tconf.REPLICAS_REMOVING_WITH_DEGRADATION,
                                                                  Suspicions.BACKUP_PRIMARY_DEGRADED.code)
    __process_backup_instance_faulty_msg_work_with_different_msgs(looper,
                                                                  txnPoolNodeSet,
                                                                  tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED,
                                                                  Suspicions.BACKUP_PRIMARY_DISCONNECTED.code)


def __process_backup_instance_faulty_msg_work_with_different_msgs(looper,
                                                                  txnPoolNodeSet,
                                                                  conf,
                                                                  suspision_code):
    node = txnPoolNodeSet[0]
    instance_to_remove = 1
    start_replicas_count = node.replicas.num_replicas
    for n in txnPoolNodeSet:
        msg = BackupInstanceFaulty(n.viewNo,
                                   [instance_to_remove],
                                   suspision_code)
        node.backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg,
                                                                                 n.name)
    if node.backup_instance_faulty_processor._is_quorum_strategy(conf):
        looper.run(eventually(check_replica_removed,
                              node,
                              start_replicas_count,
                              instance_to_remove))
    elif node.backup_instance_faulty_processor._is_local_remove_strategy(conf):
        # check that replicas were not removed
        assert all(node.requiredNumberOfInstances == node.replicas.num_replicas
                   for node in txnPoolNodeSet)
    node.backup_instance_faulty_processor.restore_replicas()


def __check_replica_removed_on_all_nodes(looper,
                                         txnPoolNodeSet,
                                         start_replicas_count,
                                         instance_to_remove):
    # check that replicas were removed
    def check_replica_removed_on_all_nodes():
        for node in txnPoolNodeSet:
            check_replica_removed(node,
                                  start_replicas_count,
                                  instance_to_remove)

    looper.run(eventually(check_replica_removed_on_all_nodes, timeout=60))
    for node in txnPoolNodeSet:
        assert not node.monitor.isMasterDegraded()
        assert len(node.requests) == 0
