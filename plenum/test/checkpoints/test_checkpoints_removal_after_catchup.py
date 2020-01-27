import pytest

from plenum.common.constants import AUDIT_LEDGER_ID, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, AUDIT_TXN_PRIMARIES
from plenum.test.checkpoints.helper import cp_key, check_num_received_checkpoints, check_last_received_checkpoint
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
            stable_cp_seq_no = replica._consensus_data.stable_checkpoint
            own_stable_checkpoints = list(replica._consensus_data.checkpoints.irange_key(min_key=stable_cp_seq_no,
                                                                                         max_key=stable_cp_seq_no))
            replica._consensus_data.checkpoints.clear()
            if len(own_stable_checkpoints) > 0:
                replica._consensus_data.checkpoints.append(own_stable_checkpoints[0])
            replica._checkpointer._received_checkpoints.clear()


def test_received_checkpoints_removed_on_master_non_primary_replica_after_catchup(
        chkFreqPatched, txnPoolNodeSet, view_setup, clear_checkpoints):

    replica = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1]
    others = set(getAllReplicas(txnPoolNodeSet, 0)) - {replica}
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica._checkpointer._mark_checkpoint_stable(10)
    replica._checkpointer._received_checkpoints[cp_key(2, 15)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 20)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 25)] = [next(iter(others)).name]

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    audit_ledger = node.getLedger(AUDIT_LEDGER_ID)
    txn_with_last_seq_no = {'txn': {'data': {AUDIT_TXN_VIEW_NO: 2,
                                             AUDIT_TXN_PP_SEQ_NO: 20,
                                             AUDIT_TXN_PRIMARIES: ['Gamma', 'Delta']}},
                            'txnMetadata': {}}
    audit_ledger.get_last_committed_txn = lambda *args: txn_with_last_seq_no
    node.write_manager.node_reg_handler.on_catchup_finished = lambda *args: None
    node.allLedgersCaughtUp()

    check_num_received_checkpoints(replica, 1)
    check_last_received_checkpoint(replica, 25, view_no=2)

    # We call _mark_stable_checkpoint for backup and expects,
    # that stable_checkpoint will be as pp_seq_no from last_caught_up
    assert replica._consensus_data.stable_checkpoint == 20


def test_received_checkpoints_removed_on_backup_non_primary_replica_after_catchup(
        chkFreqPatched, txnPoolNodeSet, view_setup, clear_checkpoints):

    replica = getNonPrimaryReplicas(txnPoolNodeSet, 1)[-1]
    others = set(getAllReplicas(txnPoolNodeSet, 1)) - {replica}
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica._checkpointer._mark_checkpoint_stable(10)
    replica._checkpointer._received_checkpoints[cp_key(2, 15)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 20)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 25)] = [next(iter(others)).name]

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    audit_ledger = node.getLedger(AUDIT_LEDGER_ID)
    txn_with_last_seq_no = {'txn': {'data': {AUDIT_TXN_VIEW_NO: 2,
                                             AUDIT_TXN_PP_SEQ_NO: 20,
                                             AUDIT_TXN_PRIMARIES: ['Gamma', 'Delta']}},
                            'txnMetadata': {}}
    audit_ledger.get_last_committed_txn = lambda *args: txn_with_last_seq_no
    node.write_manager.node_reg_handler.on_catchup_finished = lambda *args: None
    node.allLedgersCaughtUp()

    check_num_received_checkpoints(replica, 0)

    # # We don't call mark_stable_checkpoint for backup
    assert replica._consensus_data.stable_checkpoint == 10


def test_received_checkpoints_removed_on_backup_primary_replica_after_catchup(
        chkFreqPatched, txnPoolNodeSet, view_setup, clear_checkpoints):

    replica = getPrimaryReplica(txnPoolNodeSet, 1)
    others = set(getAllReplicas(txnPoolNodeSet, 1)) - {replica}
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica._consensus_data.stable_checkpoint = 15
    replica._checkpointer._received_checkpoints[cp_key(2, 20)] = [next(iter(others)).name]

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    audit_ledger = node.getLedger(AUDIT_LEDGER_ID)
    txn_with_last_seq_no = {'txn': {'data': {AUDIT_TXN_VIEW_NO: 2,
                                             AUDIT_TXN_PP_SEQ_NO: 20,
                                             AUDIT_TXN_PRIMARIES: ['Gamma', 'Delta']}},
                            'txnMetadata': {}}
    audit_ledger.get_last_committed_txn = lambda *args: txn_with_last_seq_no
    node.write_manager.node_reg_handler.on_catchup_finished = lambda *args: None
    node.allLedgersCaughtUp()

    check_num_received_checkpoints(replica, 1)
    check_last_received_checkpoint(replica, 20, view_no=2)
