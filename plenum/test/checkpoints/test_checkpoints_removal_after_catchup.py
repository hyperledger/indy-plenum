import pytest

from plenum.common.messages.node_messages import Checkpoint, CheckpointState
from plenum.test.test_node import getNonPrimaryReplicas, getAllReplicas, \
    getPrimaryReplica
from plenum.test.view_change.helper import ensure_view_change_complete

CHK_FREQ = 5


@pytest.fixture(scope="module")
def view_setup(looper, txnPoolNodeSet):
    for i in range(2):
        ensure_view_change_complete(looper, txnPoolNodeSet)
    for node in txnPoolNodeSet:
        assert node.viewNo == 2


@pytest.fixture(scope="function")
def clear_checkpoints(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        for inst_id, replica in node.replicas.items():
            replica.checkpoints.clear()
            replica.stashedRecvdCheckpoints.clear()


def test_checkpoints_removed_on_master_non_primary_replica_after_catchup(
        chkFreqPatched, txnPoolNodeSet, view_setup, clear_checkpoints):

    replica = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1]
    others = set(getAllReplicas(txnPoolNodeSet, 0)) - {replica}
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica.checkpoints[(6, 10)] = CheckpointState(seqNo=10,
                                                   digests=[],
                                                   digest='digest-6-10',
                                                   receivedDigests={r.name: 'digest-6-10' for r in others},
                                                   isStable=True)

    replica.checkpoints[(11, 15)] = CheckpointState(seqNo=12,
                                                    digests=['digest-11', 'digest-12'],
                                                    digest=None,
                                                    receivedDigests={},
                                                    isStable=False)

    replica.stashedRecvdCheckpoints[2] = {}

    replica.stashedRecvdCheckpoints[2][(11, 15)] = {}
    for r in others:
        replica.stashedRecvdCheckpoints[2][(11, 15)][r.name] = \
            Checkpoint(instId=0,
                       viewNo=2,
                       seqNoStart=11,
                       seqNoEnd=15,
                       digest='digest-11-15')

    replica.stashedRecvdCheckpoints[2][(16, 20)] = {}
    for r in others:
        replica.stashedRecvdCheckpoints[2][(16, 20)][r.name] = \
            Checkpoint(instId=0,
                       viewNo=2,
                       seqNoStart=16,
                       seqNoEnd=20,
                       digest='digest-16-20')

    replica.stashedRecvdCheckpoints[2][(21, 25)] = {}
    replica.stashedRecvdCheckpoints[2][(21, 25)][next(iter(others)).name] = \
        Checkpoint(instId=0,
                   viewNo=2,
                   seqNoStart=21,
                   seqNoEnd=25,
                   digest='digest-21-25')

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    node.allLedgersCaughtUp()

    assert len(replica.checkpoints) == 0

    assert len(replica.stashedRecvdCheckpoints) == 1
    assert 2 in replica.stashedRecvdCheckpoints
    assert len(replica.stashedRecvdCheckpoints[2]) == 1
    assert (21, 25) in replica.stashedRecvdCheckpoints[2]
    assert len(replica.stashedRecvdCheckpoints[2][(21, 25)]) == 1


def test_checkpoints_removed_on_backup_non_primary_replica_after_catchup(
        chkFreqPatched, txnPoolNodeSet, view_setup, clear_checkpoints):

    replica = getNonPrimaryReplicas(txnPoolNodeSet, 1)[-1]
    others = set(getAllReplicas(txnPoolNodeSet, 1)) - {replica}
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica.checkpoints[(6, 10)] = CheckpointState(seqNo=10,
                                                   digests=[],
                                                   digest='digest-6-10',
                                                   receivedDigests={r.name: 'digest-6-10' for r in others},
                                                   isStable=True)

    replica.checkpoints[(11, 15)] = CheckpointState(seqNo=13,
                                                    digests=['digest-11', 'digest-12', 'digest-13'],
                                                    digest=None,
                                                    receivedDigests={},
                                                    isStable=False)

    replica.stashedRecvdCheckpoints[2] = {}

    replica.stashedRecvdCheckpoints[2][(11, 15)] = {}
    for r in others:
        replica.stashedRecvdCheckpoints[2][(11, 15)][r.name] = \
            Checkpoint(instId=1,
                       viewNo=2,
                       seqNoStart=11,
                       seqNoEnd=15,
                       digest='digest-11-15')

    replica.stashedRecvdCheckpoints[2][(16, 20)] = {}
    for r in others:
        replica.stashedRecvdCheckpoints[2][(16, 20)][r.name] = \
            Checkpoint(instId=1,
                       viewNo=2,
                       seqNoStart=16,
                       seqNoEnd=20,
                       digest='digest-16-20')

    replica.stashedRecvdCheckpoints[2][(21, 25)] = {}
    replica.stashedRecvdCheckpoints[2][(21, 25)][next(iter(others)).name] = \
        Checkpoint(instId=1,
                   viewNo=2,
                   seqNoStart=21,
                   seqNoEnd=25,
                   digest='digest-21-25')

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    node.allLedgersCaughtUp()

    assert len(replica.checkpoints) == 0
    assert len(replica.stashedRecvdCheckpoints) == 0


def test_checkpoints_not_removed_on_backup_primary_replica_after_catchup(
        chkFreqPatched, txnPoolNodeSet, view_setup, clear_checkpoints):

    replica = getPrimaryReplica(txnPoolNodeSet, 1)
    others = set(getAllReplicas(txnPoolNodeSet, 1)) - {replica}
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica.checkpoints[(11, 15)] = CheckpointState(seqNo=15,
                                                   digests=[],
                                                   digest='digest-11-15',
                                                   receivedDigests={r.name: 'digest-11-15' for r in others},
                                                   isStable=True)

    replica.checkpoints[(16, 20)] = CheckpointState(seqNo=19,
                                                    digests=['digest-16', 'digest-17', 'digest-18', 'digest-19'],
                                                    digest=None,
                                                    receivedDigests={},
                                                    isStable=False)

    replica.stashedRecvdCheckpoints[2] = {}

    replica.stashedRecvdCheckpoints[2][(16, 20)] = {}
    replica.stashedRecvdCheckpoints[2][(16, 20)][next(iter(others)).name] = \
        Checkpoint(instId=1,
                   viewNo=2,
                   seqNoStart=16,
                   seqNoEnd=20,
                   digest='digest-16-20')

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    node.allLedgersCaughtUp()

    assert len(replica.checkpoints) == 2
    assert (11, 15) in replica.checkpoints
    assert (16, 20) in replica.checkpoints

    assert len(replica.stashedRecvdCheckpoints) == 1
    assert 2 in replica.stashedRecvdCheckpoints
    assert len(replica.stashedRecvdCheckpoints[2]) == 1
    assert (16, 20) in replica.stashedRecvdCheckpoints[2]
    assert len(replica.stashedRecvdCheckpoints[2][(16, 20)]) == 1
