import pytest

from plenum.common.constants import AUDIT_LEDGER_ID, AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, AUDIT_TXN_PRIMARIES
from plenum.test.checkpoints.helper import cp_key, check_num_received_checkpoints, \
    check_last_received_checkpoint, check_stable_checkpoint
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


@pytest.fixture(scope="module")
def view_change_in_progress(view_setup, txnPoolNodeSet):
    # Initiate view change to the next view
    for node in txnPoolNodeSet:
        node.view_changer.propagate_primary = False
        node.view_changer.view_no += 1
        node.view_changer.view_change_in_progress = True
        node.view_changer.previous_master_primary = node.master_primary_name
        node.view_changer.set_defaults()
        for inst_id, replica in node.replicas.items():
            replica.primaryName = None


@pytest.fixture(scope="function")
def clear_checkpoints(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        for inst_id, replica in node.replicas.items():
            replica._checkpointer._reset_checkpoints()
            replica._checkpointer._received_checkpoints.clear()


def test_checkpoints_removed_on_master_replica_after_catchup_during_view_change(
        chkFreqPatched, txnPoolNodeSet, view_change_in_progress, clear_checkpoints):

    master_replicas = getAllReplicas(txnPoolNodeSet, 0)
    replica = master_replicas[-1]
    others = master_replicas[:-1]
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica._checkpointer._mark_checkpoint_stable(10)
    replica._checkpointer._received_checkpoints[cp_key(2, 15)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 20)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 25)] = [others[0].name]

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    audit_ledger = node.getLedger(AUDIT_LEDGER_ID)
    txn_with_last_seq_no = {'txn': {'data': {AUDIT_TXN_VIEW_NO: 2,
                                             AUDIT_TXN_PP_SEQ_NO: 20,
                                             AUDIT_TXN_PRIMARIES: ['Gamma', 'Delta']}}}
    audit_ledger.get_last_committed_txn = lambda *args: txn_with_last_seq_no
    node.allLedgersCaughtUp()

    check_num_received_checkpoints(replica, 1)
    check_last_received_checkpoint(replica, 25, view_no=2)

    # TODO: This wasn't checked in original test, but most probably it should. And now this fails.
    check_stable_checkpoint(replica, 20)


def test_checkpoints_removed_on_backup_replica_after_catchup_during_view_change(
        chkFreqPatched, txnPoolNodeSet, view_change_in_progress, clear_checkpoints):

    backup_replicas = getAllReplicas(txnPoolNodeSet, 1)
    replica = backup_replicas[-1]
    others = backup_replicas[:-1]
    node = replica.node

    node.master_replica.last_ordered_3pc = (2, 12)

    replica._checkpointer._mark_checkpoint_stable(10)
    replica._checkpointer._received_checkpoints[cp_key(2, 15)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 20)] = [r.name for r in others]
    replica._checkpointer._received_checkpoints[cp_key(2, 25)] = [others[0].name]

    # Simulate catch-up completion
    node.ledgerManager.last_caught_up_3PC = (2, 20)
    audit_ledger = node.getLedger(AUDIT_LEDGER_ID)
    txn_with_last_seq_no = {'txn': {'data': {AUDIT_TXN_VIEW_NO: 2,
                                             AUDIT_TXN_PP_SEQ_NO: 20,
                                             AUDIT_TXN_PRIMARIES: ['Gamma', 'Delta']}}}
    audit_ledger.get_last_committed_txn = lambda *args: txn_with_last_seq_no
    node.allLedgersCaughtUp()

    check_num_received_checkpoints(replica, 0)
