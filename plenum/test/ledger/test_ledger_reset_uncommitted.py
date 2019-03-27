def test_reset_uncommitted(ledger_with_batches_appended, inital_size, inital_root_hash):
    ledger = ledger_with_batches_appended
    ledger.reset_uncommitted()
    assert ledger.uncommitted_size == inital_size
    assert ledger.uncommittedRootHash is None
    assert ledger.uncommitted_root_hash == inital_root_hash
    assert ledger.uncommittedTxns == []
