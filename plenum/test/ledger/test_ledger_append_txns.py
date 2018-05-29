from plenum.common.txn_util import get_seq_no
from plenum.test.ledger.conftest import NUM_BATCHES, TXNS_IN_BATCH


def test_append_txns_does_not_changed_committed(ledger_with_batches_appended):
    ledger = ledger_with_batches_appended
    committed_size = ledger.size
    committed_root = ledger.root_hash

    assert committed_size == ledger.size
    assert committed_root == ledger.root_hash


def test_append_txns_uncommitted_size(ledger_with_batches_appended):
    ledger = ledger_with_batches_appended
    assert len(ledger.uncommittedTxns) == NUM_BATCHES * TXNS_IN_BATCH
    assert ledger.uncommitted_size == NUM_BATCHES * TXNS_IN_BATCH
    assert ledger.uncommittedRootHash != ledger.root_hash


def test_append_txns_uncommitted_root(ledger_with_batches_appended):
    ledger = ledger_with_batches_appended
    assert ledger.uncommittedRootHash != ledger.root_hash


def test_append_txns_correct_seq_nos(ledger_with_batches_appended):
    ledger = ledger_with_batches_appended
    seq_no = ledger.seqNo
    for txn in ledger.uncommittedTxns:
        seq_no += 1
        assert get_seq_no(txn) == seq_no
