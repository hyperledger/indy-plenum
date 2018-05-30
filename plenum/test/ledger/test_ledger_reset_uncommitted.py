def test_reset_uncommitted(ledger_with_batches_appended):
    ledger = ledger_with_batches_appended
    ledger.reset_uncommitted()
    assert ledger.uncommitted_size == 0
    assert ledger.uncommittedRootHash is None
    assert ledger.uncommittedTxns == []
