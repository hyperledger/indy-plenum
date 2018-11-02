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
    node.allNodeNames = ["Node{}".format(i)
                         for i in range(1, (node.requiredNumberOfInstances - 1) * 3 - 1)]
    node.name = node.allNodeNames[0]
    replicas = node.replicas
    node.replicas = FakeSomething(
        remove_replica=lambda a: replicas.pop(a),
        add_replica=lambda a: None,
        items=lambda: replicas.items(),
        keys=lambda: replicas.keys()
    )
    node.backup_instance_faulty_processor = BackupInstanceFaultyProcessor(node)
    return node.backup_instance_faulty_processor


# tests for on_backup_degradation

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


# tests for on_backup_primary_disconnected


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


# tests for restore_replicas


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


# tests for process_backup_instance_faulty_msg


def test_process_backup_instance_empty_msg(backup_instance_faulty_processor):
    '''
    Check that BackupInstanceFaulty message with empty list of instances will not be processed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    msg = BackupInstanceFaulty(node.viewNo,
                               [],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg, node.name)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    # TODO: spylog for replica_remove


def test_process_backup_instance_faulty_incorrect_view_no(backup_instance_faulty_processor):
    '''
    Check that BackupInstanceFaulty message with incorrect viewNo will not be processed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    msg = BackupInstanceFaulty(node.viewNo + 1,
                               [1, 2],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg, node.name)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    # TODO: spylog for replica_remove


def test_process_backup_instance_faulty_msg_local_degradation(backup_instance_faulty_processor):
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "local"
    msg = BackupInstanceFaulty(node.viewNo,
                               [1, 2],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg, node.name)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    # TODO: spylog for replica_remove


def test_process_backup_instance_faulty_msg_quorum_degradation(backup_instance_faulty_processor):
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    instance_to_remove = 1
    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code)
    nodes = set()
    # check that node.quorums.backup_instance_faulty - 1 messages don't leads to replica removing
    for i in range(node.quorums.backup_instance_faulty - 1):
        node_name = node.allNodeNames[i]
        nodes.add(node_name)
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg, node_name)

    assert nodes.issubset(backup_instance_faulty_processor.backup_instances_faulty[instance_to_remove])
    # TODO: spylog for replica_remove

    # check that messages from all nodes lead to replica removing
    for i in range(node.quorums.backup_instance_faulty - 1):
        node_name = node.allNodeNames[i]
        nodes.add(node_name)
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg, node_name)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    # TODO: spylog for replica_remove
