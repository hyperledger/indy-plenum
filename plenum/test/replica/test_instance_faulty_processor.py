from contextlib import ExitStack
import pytest

from plenum.common.messages.node_messages import BackupInstanceFaulty
from plenum.common.types import f
from plenum.server.backup_instance_faulty_processor import BackupInstanceFaultyProcessor
from plenum.server.suspicion_codes import Suspicions
from plenum.test.primary_selection.test_primary_selector import FakeNode
from plenum.test.replica.helper import check_replica_removed
from plenum.test.testing_utils import FakeSomething
from stp_core.loop.eventually import eventually
from plenum.test.helper import waitForViewChange, create_new_test_node
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected


@pytest.fixture(scope="function")
def backup_instance_faulty_processor(tdir, tconf):
    node = FakeNode(tdir, config=tconf)
    node.view_change_in_progress = False
    node.requiredNumberOfInstances = len(node.replicas)
    replicas = node.replicas
    node.replicas = FakeSomething(
        remove_replica=lambda a: replicas.pop(a),
        add_replica=lambda a: None,
        items=lambda: replicas.items(),
        keys=lambda: replicas.keys()
    )
    node.backup_instance_faulty_processor = BackupInstanceFaultyProcessor(node)
    return node.backup_instance_faulty_processor


def test_on_backup_degradation_local(looper,
                                     backup_instance_faulty_processor):
    '''
    1. Call on_backup_degradation() with local strategy for backup performance degradation
    2. Check that set of degraded instances which was send in on_backup_degradation
    equals with set in remove_replica call.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "local"
    degraded_backups = {1, 2}
    removed_backups = set()

    def remove_replica(inst_ids):
        removed_backups.add(inst_ids)
    node.replicas.remove_replica = remove_replica

    backup_instance_faulty_processor.on_backup_degradation(list(degraded_backups))

    assert not (removed_backups - degraded_backups)
    assert not backup_instance_faulty_processor.backup_instances_faulty


def test_on_backup_degradation_quorum(looper,
                                      backup_instance_faulty_processor):
    '''
    1. Call on_backup_degradation() with quorum strategy for backup performance degradation
    2. Check that correct message BackupInstanceFaulty was sending.
    2. Check that own message BackupInstanceFaulty has been added backup_instances_faulty.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"

    def send(msg):
        assert isinstance(msg, BackupInstanceFaulty)
        assert getattr(msg, f.VIEW_NO.nm) == 0
        assert getattr(msg, f.INSTANCES.nm) == degraded_backups
        assert getattr(msg, f.REASON.nm) == Suspicions.BACKUP_PRIMARY_DEGRADED.code
    node.send = send
    degraded_backups = [1, 2]

    backup_instance_faulty_processor.on_backup_degradation(degraded_backups)

    assert all(node.name in backup_instance_faulty_processor.backup_instances_faulty[inst_id]
               for inst_id in degraded_backups)


def test_on_backup_primary_disconnected_local(looper,
                                     backup_instance_faulty_processor):
    '''
    1. Call on_backup_primary_disconnected() with local strategy for backup primary disconnected.
    2. Check that set of degraded instances which was send in on_backup_degradation.
    equals with set in remove_replica call.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "local"
    degraded_backups = {1, 2}
    removed_backups = set()

    def remove_replica(inst_ids):
        removed_backups.add(inst_ids)
    node.replicas.remove_replica = remove_replica

    backup_instance_faulty_processor.on_backup_primary_disconnected(list(degraded_backups))

    assert not (removed_backups - degraded_backups)
    assert not backup_instance_faulty_processor.backup_instances_faulty


def test_on_backup_primary_disconnected_quorum(looper,
                                      backup_instance_faulty_processor):
    '''
    1. Call on_backup_primary_disconnected() with quorum strategy for backup primary disconnected.
    2. Check that correct message BackupInstanceFaulty was sending.
    2. Check that own message BackupInstanceFaulty has been added backup_instances_faulty.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "quorum"

    def send(msg):
        assert isinstance(msg, BackupInstanceFaulty)
        assert getattr(msg, f.VIEW_NO.nm) == 0
        assert getattr(msg, f.INSTANCES.nm) == degraded_backups
        assert getattr(msg, f.REASON.nm) == Suspicions.BACKUP_PRIMARY_DISCONNECTED.code
    node.send = send
    degraded_backups = [1, 2]

    backup_instance_faulty_processor.on_backup_primary_disconnected(degraded_backups)

    assert all(node.name in backup_instance_faulty_processor.backup_instances_faulty[inst_id]
               for inst_id in degraded_backups)


def test_restore_replicas(backup_instance_faulty_processor):
    '''
    1. Remove 1, 2 backup instances
    2. Call restore_replicas()
    3. Check that add_replica() call for all removed replicas
    '''
    node = backup_instance_faulty_processor.node
    restored_replicas = set()
    removed_replicas = {1, 2}
    for r in removed_replicas:
        node.replicas.remove_replica(r)

    def add_replica(inst_ids):
        restored_replicas.add(inst_ids)
    node.replicas.add_replica = add_replica

    backup_instance_faulty_processor.restore_replicas()
    # check that all replicas were restored and backup_instances_faulty has been cleaned
    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not (removed_replicas - restored_replicas)


def test_restore_replicas_when_nothing_is_removed(backup_instance_faulty_processor):
    '''
    1. Call restore_replicas()
    3. Check that method didn't add new replicas.
    '''
    node = backup_instance_faulty_processor.node
    restored_replicas = set()

    def add_replica(inst_ids):
        restored_replicas.add(inst_ids)
    node.replicas.add_replica = add_replica

    backup_instance_faulty_processor.restore_replicas()

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not restored_replicas


def test_process_backup_instance_empty_msg(backup_instance_faulty_processor):
    node = backup_instance_faulty_processor.node
    msg = BackupInstanceFaulty(0,
                               [],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg, node.name)

    assert not backup_instance_faulty_processor.backup_instances_faulty


def test_process_backup_instance_faulty_msg(looper,
                                            txnPoolNodeSet,
                                            tconf,
                                            do_view_change):
    __process_backup_instance_faulty_msg_work_with_different_msgs(looper,
                                                                  txnPoolNodeSet,
                                                                  tconf.REPLICAS_REMOVING_WITH_DEGRADATION,
                                                                  Suspicions.BACKUP_PRIMARY_DEGRADED.code)
    __process_backup_instance_faulty_msg_work_with_different_msgs(looper,
                                                                  txnPoolNodeSet,
                                                                  tconf.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED,
                                                                  Suspicions.BACKUP_PRIMARY_DISCONNECTED.code)



def test_process_backup_instance_with_incorrect_view_no(looper,
                                                        txnPoolNodeSet,
                                                        tconf,
                                                        do_view_change):
    node = txnPoolNodeSet[0]
    for n in txnPoolNodeSet:
        msg = BackupInstanceFaulty(n.viewNo + 1,
                                   [1],
                                   Suspicions.BACKUP_PRIMARY_DEGRADED.code)
        node.backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg,
                                                                                 n.name)
    assert all(n.requiredNumberOfInstances == n.replicas.num_replicas
               for n in txnPoolNodeSet)


def __process_backup_instance_faulty_msg_work_with_different_msgs(looper,
                                                                  txnPoolNodeSet,
                                                                  conf,
                                                                  suspision_code):
    node = txnPoolNodeSet[0]
    instance_to_remove = 1
    start_replicas_count = node.replicas.num_replicas
    number_sent = 0
    for n in txnPoolNodeSet:
        if n.name == node.name:
            continue
        msg = BackupInstanceFaulty(n.viewNo,
                                   [instance_to_remove],
                                   suspision_code)
        node.backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg,
                                                                                 n.name)
        number_sent += 1
        if number_sent < node.quorums.backup_instance_faulty.value:
            assert node.requiredNumberOfInstances == node.replicas.num_replicas

    if node.backup_instance_faulty_processor._is_quorum_strategy(conf):
        looper.run(eventually(check_replica_removed,
                              node,
                              start_replicas_count,
                              instance_to_remove))
    elif node.backup_instance_faulty_processor._is_local_remove_strategy(conf):
        # check that replicas were not removed
        assert all(n.requiredNumberOfInstances == n.replicas.num_replicas
                   for n in txnPoolNodeSet)
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
