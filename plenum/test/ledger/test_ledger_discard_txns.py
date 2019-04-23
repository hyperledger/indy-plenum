import pytest

from common.exceptions import LogicError
from plenum.test.ledger.conftest import create_txns, TXNS_IN_BATCH


def test_discard_notempty_no_uncommitted(ledger):
    with pytest.raises(LogicError):
        ledger.discardTxns(1)


def test_discard_empty_no_uncommitted(ledger):
    initial_seq_no = ledger.seqNo
    initial_size = ledger.size
    initial_root = ledger.root_hash
    ledger.discardTxns(0)
    assert ledger.size == initial_size
    assert ledger.root_hash == initial_root
    assert ledger.seqNo == initial_seq_no
    assert ledger.uncommittedRootHash is None
    assert ledger.uncommitted_size == ledger.size


def test_discard_txns(ledger,
                      looper, sdk_wallet_client):
    txns1 = create_txns(looper, sdk_wallet_client)
    ledger.append_txns_metadata(txns1)
    ledger.appendTxns(txns1)
    root1 = ledger.uncommittedRootHash

    txns2 = create_txns(looper, sdk_wallet_client)
    ledger.append_txns_metadata(txns2)
    ledger.appendTxns(txns2)
    root2 = ledger.uncommittedRootHash

    initial_seq_no = ledger.seqNo
    initial_size = ledger.size
    initial_root = ledger.root_hash
    initial_uncommitted_size = ledger.uncommitted_size
    initial_uncommitted_root = ledger.uncommittedRootHash
    assert initial_uncommitted_size == initial_size + TXNS_IN_BATCH * 2
    assert initial_uncommitted_root == root2

    # discard zero
    ledger.discardTxns(0)
    assert ledger.size == initial_size
    assert ledger.root_hash == initial_root
    assert ledger.seqNo == initial_seq_no
    assert ledger.uncommittedRootHash == initial_uncommitted_root
    assert ledger.uncommitted_size == initial_uncommitted_size

    # discard first batch
    ledger.discardTxns(TXNS_IN_BATCH)
    assert ledger.uncommitted_size == ledger.size + TXNS_IN_BATCH
    assert ledger.size == initial_size
    assert len(ledger.uncommittedTxns) == TXNS_IN_BATCH
    assert ledger.uncommittedRootHash == root1
    assert ledger.root_hash == initial_root

    # discard second batch
    ledger.discardTxns(TXNS_IN_BATCH)
    assert ledger.uncommitted_size == ledger.size
    assert ledger.size == initial_size
    assert len(ledger.uncommittedTxns) == 0
    assert ledger.uncommittedRootHash is None
    assert ledger.root_hash == initial_root
