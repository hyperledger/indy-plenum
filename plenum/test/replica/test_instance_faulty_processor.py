import pytest

from plenum.common.messages.node_messages import BackupInstanceFaulty
from plenum.common.types import f
from plenum.server.backup_instance_faulty_processor import BackupInstanceFaultyProcessor
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.server.suspicion_codes import Suspicions
from plenum.test.primary_selection.test_primary_selector import FakeNode
from plenum.test.testing_utils import FakeSomething


class FakeReplicas:
    def __init__(self, node, replicas):
        self._replicas = replicas
        self._node = node
        self.remove_replica_calls = []
        self.add_replica_calls = []

        self.items = lambda: replicas.items()
        self.keys = lambda: replicas.keys()

    def remove_replica(self, inst_id):
        self.remove_replica_calls.append(inst_id)
        self._replicas.pop(inst_id)

    def add_replica(self, inst_id):
        self.add_replica_calls.append(inst_id)
        self._replicas.update(inst_id=Replica(node=self._node, instId=inst_id))

    def __getitem__(self, item):
        return self._replicas[item]


@pytest.fixture(scope="function")
def backup_instance_faulty_processor(tdir, tconf):
    node = FakeNode(tdir, config=tconf)
    node.view_change_in_progress = False
    node.requiredNumberOfInstances = len(node.replicas)
    node.allNodeNames = ["Node{}".format(i)
                         for i in range(1, (node.requiredNumberOfInstances - 1) * 3 + 2)]
    node.totalNodes = len(node.allNodeNames)
    node.quorums = Quorums(node.totalNodes)
    node.name = node.allNodeNames[0]
    node.replicas = FakeReplicas(node, node.replicas)
    node.backup_instance_faulty_processor = BackupInstanceFaultyProcessor(node)
    return node.backup_instance_faulty_processor


# TESTS FOR ON_BACKUP_DEGRADATION


def test_on_backup_degradation_local(looper,
                                     backup_instance_faulty_processor):
    '''
    1. Call on_backup_degradation() with local strategy for backup performance degradation
    2. Check that set of degraded instances which was send in on_backup_degradation
    equals with set in remove_replica call.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "local"
    degraded_backups = [1, 2]

    backup_instance_faulty_processor.on_backup_degradation(degraded_backups)

    assert not (set(node.replicas.remove_replica_calls) - set(degraded_backups))
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


# TESTS FOR ON_BACKUP_PRIMARY_DISCONNECTED


def test_on_backup_primary_disconnected_local(looper,
                                              backup_instance_faulty_processor):
    '''
    1. Call on_backup_primary_disconnected() with local strategy for backup primary disconnected.
    2. Check that set of degraded instances which was send in on_backup_degradation.
    equals with set in remove_replica call.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "local"
    degraded_backups = [1, 2]

    backup_instance_faulty_processor.on_backup_primary_disconnected(degraded_backups)

    assert not (set(node.replicas.remove_replica_calls) - set(degraded_backups))
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


# TESTS FOR RESTORE_REPLICAS


def test_restore_replicas(backup_instance_faulty_processor):
    '''
    1. Remove 1, 2 backup instances
    2. Call restore_replicas()
    3. Check that add_replica() call for all removed replicas
    '''
    node = backup_instance_faulty_processor.node
    removed_replicas = {1, 2}
    for r in removed_replicas:
        node.replicas.remove_replica(r)

    backup_instance_faulty_processor.restore_replicas()
    # check that all replicas were restored and backup_instances_faulty has been cleaned
    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not (removed_replicas - set(node.replicas.add_replica_calls))


def test_restore_replicas_when_nothing_is_removed(backup_instance_faulty_processor):
    '''
    1. Call restore_replicas()
    3. Check that method didn't add new replicas.
    '''
    node = backup_instance_faulty_processor.node
    backup_instance_faulty_processor.restore_replicas()

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not node.replicas.add_replica_calls


# TESTS FOR PROCESS_BACKUP_INSTANCE_FAULTY_MSG


def test_process_backup_instance_empty_msg(backup_instance_faulty_processor):
    '''
    Check that BackupInstanceFaulty message with empty list of instances will not be processed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    msg = BackupInstanceFaulty(node.viewNo,
                               [],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code,
                               frm=node.name)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not node.replicas.remove_replica_calls


def test_process_backup_instance_faulty_incorrect_view_no(backup_instance_faulty_processor):
    '''
    Check that BackupInstanceFaulty message with incorrect viewNo will not be processed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    msg = BackupInstanceFaulty(node.viewNo + 1,
                               [1, 2],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code,
                               frm=node.name)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not node.replicas.remove_replica_calls


def test_process_backup_instance_faulty_msg_contains_master_instance(backup_instance_faulty_processor):
    '''
    Check that BackupInstanceFaulty message with master instance will not be processed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    msg = BackupInstanceFaulty(node.viewNo,
                               [1, 0],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code,
                               frm=node.name)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not node.replicas.remove_replica_calls


def test_process_backup_instance_faulty_msg_local_degradation(backup_instance_faulty_processor):
    '''
    Check that BackupInstanceFaulty message with REPLICAS_REMOVING_WITH_DEGRADATION
    will not be processed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "local"
    msg = BackupInstanceFaulty(node.viewNo,
                               [1, 2],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code,
                               frm=node.name)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not node.replicas.remove_replica_calls


def test_process_backup_instance_faulty_msg_quorum_degradation(backup_instance_faulty_processor):
    '''
    Send (quorum - 1) of messages with BACKUP_PRIMARY_DEGRADED from different nodes and
    check that it is not lead to a replica removing. But after sending one more message,
    the replica will be removed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    instance_to_remove = 1
    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code,
                               frm=node_name)
    nodes = set()
    # check that node.quorums.backup_instance_faulty - 1 messages don't leads to replica removing
    for node_name in node.allNodeNames[:node.quorums.backup_instance_faulty.value - 1]:
        nodes.add(node_name)
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert nodes.issubset(backup_instance_faulty_processor.backup_instances_faulty[instance_to_remove])
    assert not node.replicas.remove_replica_calls

    # check that messages from all nodes lead to replica removing
    for node_name in node.allNodeNames[node.quorums.backup_instance_faulty.value - 1:]:
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert len(node.replicas.remove_replica_calls) == 1
    assert node.replicas.remove_replica_calls[0] == instance_to_remove


def test_process_backup_instance_faulty_msg_local_disconnection(backup_instance_faulty_processor):
    '''
    Check that BackupInstanceFaulty message with REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED
    will not be processed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "local"
    msg = BackupInstanceFaulty(node.viewNo,
                               [1, 2],
                               Suspicions.BACKUP_PRIMARY_DISCONNECTED.code,
                               frm=node.name)

    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert not node.replicas.remove_replica_calls


def test_process_backup_instance_faulty_msg_quorum_disconnection(backup_instance_faulty_processor):
    '''
    Send quorum of messages with BACKUP_PRIMARY_DISCONNECTED from different nodes.
    Replica should be removed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "quorum"
    instance_to_remove = 1
    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove],
                               Suspicions.BACKUP_PRIMARY_DISCONNECTED.code)
    for node_name in node.allNodeNames:
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg, node_name)

    # check that messages from all nodes lead to replica removing
    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert len(node.replicas.remove_replica_calls) == 1
    assert node.replicas.remove_replica_calls[0] == instance_to_remove


def test_process_backup_instance_faulty_msg_quorum_from_others(backup_instance_faulty_processor):
    '''
    Send quorum of messages from different nodes exception current node.
    Replica should be removed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "quorum"
    instance_to_remove = 1
    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove],
                               Suspicions.BACKUP_PRIMARY_DISCONNECTED.code,
                               frm=node_name)
    # send messages from all nodes with exception of current node
    for node_name in node.allNodeNames[1:]:
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    # check that remove_replica was called
    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert len(node.replicas.remove_replica_calls) == 1
    assert node.replicas.remove_replica_calls[0] == instance_to_remove


def test_process_backup_instance_faulty_msg_quorum_from_itself(backup_instance_faulty_processor):
    '''
    Send quorum of messages from current node.
    Replica should be removed.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "quorum"
    instance_to_remove = 1
    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove],
                               Suspicions.BACKUP_PRIMARY_DISCONNECTED.code,
                               frm=node.name)
    # check that node.quorums.backup_instance_faulty own messages lead to replica removing
    for _ in range(node.quorums.backup_instance_faulty.value):
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not backup_instance_faulty_processor.backup_instances_faulty
    assert len(node.replicas.remove_replica_calls) == 1
    assert node.replicas.remove_replica_calls[0] == instance_to_remove


def test_process_backup_instance_faulty_without_quorum(backup_instance_faulty_processor):
    '''
    Send (quorum - 1) messages from different nodes and (quorum - 1) from current node.
    Replica shouldn't be removed in both cases.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_DEGRADATION = "quorum"
    instance_to_remove = 1
    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove],
                               Suspicions.BACKUP_PRIMARY_DEGRADED.code,
                               frm=node.name)
    nodes = set()
    # check that node.quorums.backup_instance_faulty - 1 messages don't leads to replica removing
    for node_name in node.allNodeNames[1:node.quorums.backup_instance_faulty.value - 1]:
        nodes.add(node_name)
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert nodes.issubset(backup_instance_faulty_processor.backup_instances_faulty[instance_to_remove].keys())
    assert not node.replicas.remove_replica_calls

    # check that node.quorums.backup_instance_faulty - 1 own messages don't lead to replica removing
    for _ in range(node.quorums.backup_instance_faulty.value - 1):
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert nodes.issubset(backup_instance_faulty_processor.backup_instances_faulty[instance_to_remove].keys())
    assert backup_instance_faulty_processor.backup_instances_faulty[instance_to_remove][node.name] == \
           node.quorums.backup_instance_faulty.value - 1
    assert not node.replicas.remove_replica_calls


def test_process_backup_instance_faulty_msg_quorum_for_different_replicas(backup_instance_faulty_processor):
    '''
    Test for case when process_backup_instance_faulty_msg received messages for different instances.
    Firstly f messages for [1,2], secondly 1 message for [2]. As a result, replica 2 should be removed
    and replica 1 are not.
    '''
    node = backup_instance_faulty_processor.node
    node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED = "quorum"
    instance_to_remove = 2
    instance_not_removed = 1
    nodes = set()
    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove, instance_not_removed],
                               Suspicions.BACKUP_PRIMARY_DISCONNECTED.code,
                               frm=node_name)
    # send node.quorums.backup_instance_faulty - 1 BackupInstanceFaulty messages for 1, 2 replicas
    for node_name in node.allNodeNames[:node.quorums.backup_instance_faulty.value - 1]:
        nodes.add(node_name)
        backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    assert not node.replicas.remove_replica_calls

    msg = BackupInstanceFaulty(node.viewNo,
                               [instance_to_remove],
                               Suspicions.BACKUP_PRIMARY_DISCONNECTED.code,
                               frm=node_name)
    # send BackupInstanceFaulty message for 2 replica
    node_name = node.allNodeNames[node.quorums.backup_instance_faulty.value - 1]
    backup_instance_faulty_processor.process_backup_instance_faulty_msg(msg)

    # check that 2nd replica was removed and 1st replica did not.
    assert instance_to_remove not in backup_instance_faulty_processor.backup_instances_faulty
    assert len(node.replicas.remove_replica_calls) == 1
    assert node.replicas.remove_replica_calls[0] == instance_to_remove
    assert nodes.issubset(backup_instance_faulty_processor.backup_instances_faulty[instance_not_removed].keys())


def test_quorum_collection(tdir):
    node = FakeNode(tdir)
    proc = BackupInstanceFaultyProcessor(node)
    backup_faulty = BackupInstanceFaulty(0, [1], Suspicions.BACKUP_PRIMARY_DEGRADED.code, frm='someone')
    proc.process_backup_instance_faulty_msg(backup_faulty)

    assert len(proc.backup_instances_faulty[1].keys()) == 1
