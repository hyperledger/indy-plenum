import pytest

from common.exceptions import PlenumValueError
from plenum.common.txn_util import get_seq_no
from plenum.common.constants import TXN_METADATA, TXN_METADATA_SEQ_NO
from plenum.test.ledger.conftest import NUM_BATCHES, TXNS_IN_BATCH, create_txns


def test_ledger_appendTxns_args(ledger, looper, sdk_wallet_client):
    txns = create_txns(looper, sdk_wallet_client)

    # None seq_no
    txns[0][TXN_METADATA][TXN_METADATA_SEQ_NO] = None
    with pytest.raises(PlenumValueError):
        ledger.appendTxns(txns)

    # missed seq_no
    del txns[0][TXN_METADATA][TXN_METADATA_SEQ_NO]
    with pytest.raises(PlenumValueError):
        ledger.appendTxns(txns)


def test_append_txns_does_not_changed_committed(ledger_with_batches_appended):
    ledger = ledger_with_batches_appended
    committed_size = ledger.size
    committed_root = ledger.root_hash

    assert committed_size == ledger.size
    assert committed_root == ledger.root_hash


def test_append_txns_uncommitted_size(ledger_with_batches_appended, inital_size):
    ledger = ledger_with_batches_appended
    assert len(ledger.uncommittedTxns) == NUM_BATCHES * TXNS_IN_BATCH
    assert ledger.uncommitted_size == inital_size + NUM_BATCHES * TXNS_IN_BATCH
    assert ledger.uncommittedRootHash != ledger.root_hash


def test_append_txns_uncommitted_root(ledger_with_batches_appended, inital_root_hash):
    ledger = ledger_with_batches_appended
    assert ledger.uncommittedRootHash != ledger.tree.root_hash
    assert ledger.uncommitted_root_hash == ledger.uncommittedRootHash


def test_append_txns_correct_seq_nos(ledger_with_batches_appended):
    ledger = ledger_with_batches_appended
    seq_no = ledger.seqNo
    for txn in ledger.uncommittedTxns:
        seq_no += 1
        assert get_seq_no(txn) == seq_no
