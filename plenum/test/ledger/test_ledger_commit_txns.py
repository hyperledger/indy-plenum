from plenum.test.ledger.conftest import create_txns, TXNS_IN_BATCH


def test_commit_empty(ledger, inital_root_hash):
    initial_seq_no = ledger.seqNo
    initial_size = ledger.size
    initial_root = ledger.root_hash
    (start, end), committed_txns = ledger.commitTxns(0)
    assert start == initial_seq_no
    assert end == initial_seq_no
    assert len(committed_txns) == 0
    assert ledger.size == initial_size
    assert ledger.root_hash == initial_root
    assert ledger.uncommitted_root_hash == inital_root_hash


def test_commit_txns(ledger,
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
    assert ledger.uncommitted_size == initial_size + TXNS_IN_BATCH * 2

    # commit first batch
    (start, end), committed_txns = ledger.commitTxns(TXNS_IN_BATCH)
    assert start == initial_seq_no + 1
    assert end == initial_seq_no + TXNS_IN_BATCH
    assert len(committed_txns) == TXNS_IN_BATCH
    assert ledger.uncommitted_size == ledger.size + TXNS_IN_BATCH
    assert ledger.size == initial_size + TXNS_IN_BATCH
    assert len(ledger.uncommittedTxns) == TXNS_IN_BATCH
    assert ledger.uncommittedRootHash == root2
    assert ledger.uncommitted_root_hash == root2
    assert ledger.tree.root_hash == root1

    # commit second batch
    (start, end), committed_txns = ledger.commitTxns(TXNS_IN_BATCH)
    assert start == initial_seq_no + TXNS_IN_BATCH + 1
    assert end == initial_seq_no + TXNS_IN_BATCH + TXNS_IN_BATCH
    assert len(committed_txns) == TXNS_IN_BATCH
    assert ledger.uncommitted_size == ledger.size
    assert ledger.size == initial_size + 2 * TXNS_IN_BATCH
    assert len(ledger.uncommittedTxns) == 0
    assert ledger.uncommittedRootHash is None
    assert ledger.uncommitted_root_hash == root2
    assert ledger.tree.root_hash == root2
