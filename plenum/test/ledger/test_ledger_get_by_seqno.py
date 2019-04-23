import pytest

from plenum.test.ledger.conftest import TXNS_IN_BATCH, NUM_BATCHES


def test_get_unexistent_txn(ledger_with_batches_appended, inital_size):
    ledger = ledger_with_batches_appended
    with pytest.raises(KeyError):
        ledger.get_by_seq_no_uncommitted(inital_size + NUM_BATCHES * TXNS_IN_BATCH + 1)
    with pytest.raises(KeyError):
        ledger.get_by_seq_no_uncommitted(0)
    with pytest.raises(KeyError):
        ledger.get_by_seq_no_uncommitted(-1)


def test_get_committed_txns(ledger_with_batches_appended, inital_size):
    ledger = ledger_with_batches_appended
    assert ledger.get_by_seq_no_uncommitted(1)
    assert ledger.get_by_seq_no_uncommitted(inital_size)


def test_get_uncommitted_txns(ledger_with_batches_appended, inital_size, created_txns):
    ledger = ledger_with_batches_appended
    assert ledger.get_by_seq_no_uncommitted(inital_size + 1) == created_txns[0][0]
    assert ledger.get_by_seq_no_uncommitted(inital_size + 2) == created_txns[0][1]
    assert ledger.get_by_seq_no_uncommitted(inital_size + TXNS_IN_BATCH) == created_txns[0][TXNS_IN_BATCH - 1]
    assert ledger.get_by_seq_no_uncommitted(inital_size + TXNS_IN_BATCH + 3) == created_txns[1][2]
    assert ledger.get_by_seq_no_uncommitted(inital_size + (NUM_BATCHES - 1) * TXNS_IN_BATCH) == \
           created_txns[NUM_BATCHES - 2][TXNS_IN_BATCH - 1]
    assert ledger.get_by_seq_no_uncommitted(inital_size + (NUM_BATCHES - 1) * TXNS_IN_BATCH + 1) == \
           created_txns[NUM_BATCHES - 1][0]
    assert ledger.get_by_seq_no_uncommitted(inital_size + NUM_BATCHES * TXNS_IN_BATCH) == \
           created_txns[NUM_BATCHES - 1][TXNS_IN_BATCH - 1]
