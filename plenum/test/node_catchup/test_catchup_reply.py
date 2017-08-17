def test_catchup_reply_merge(txnPoolNodeSet):
    """
    Testing LedgerManager's `_get_merged_catchup_txns`
    """
    ledger_manager = txnPoolNodeSet[0].ledgerManager

    # Without overlap
    existing_txns = [(i, {}) for i in range(1, 11)]
    new_txns = [(i, {}) for i in range(11, 16)]
    merged = ledger_manager._get_merged_catchup_txns(existing_txns, new_txns)
    assert [(i, {}) for i in range(1, 16)] == merged

    # With partial overlap
    existing_txns = [(i, {}) for i in range(1, 13)]
    new_txns = [(i, {}) for i in range(11, 16)]
    merged = ledger_manager._get_merged_catchup_txns(existing_txns, new_txns)
    assert [(i, {}) for i in range(1, 16)] == merged

    # With complete overlap
    existing_txns = [(i, {}) for i in range(1, 21)]
    new_txns = [(i, {}) for i in range(11, 16)]
    merged = ledger_manager._get_merged_catchup_txns(existing_txns, new_txns)
    assert [(i, {}) for i in range(1, 21)] == merged

    # existing_txns has a gap and new_txns overlap partially with an interval
    # in existing_txns
    existing_txns = [(i, {}) for i in range(1, 11)] + [(i, {})
                                                       for i in range(20, 41)]
    new_txns = [(i, {}) for i in range(15, 29)]
    merged = ledger_manager._get_merged_catchup_txns(existing_txns, new_txns)
    assert ([(i, {}) for i in range(1, 11)] +
            [(i, {}) for i in range(15, 41)]) == merged

    # existing_txns has a gap and new_txns overlap completely with an interval
    # in existing_txns
    existing_txns = [(i, {}) for i in range(1, 11)] + \
                    [(i, {}) for i in range(20, 31)] + \
                    [(i, {}) for i in range(41, 51)]
    new_txns = [(i, {}) for i in range(15, 33)]
    merged = ledger_manager._get_merged_catchup_txns(existing_txns, new_txns)
    assert ([(i, {}) for i in range(1, 11)] +
            [(i, {}) for i in range(15, 33)] +
            [(i, {}) for i in range(41, 51)]) == merged

    # existing_txns has a gap and new_txns overlap completely with multiple
    # intervals in existing_txns
    existing_txns = [(i, {}) for i in range(1, 11)] + \
                    [(i, {}) for i in range(20, 31)] + \
                    [(i, {}) for i in range(41, 51)] + \
                    [(i, {}) for i in range(61, 95)]
    new_txns = [(i, {}) for i in range(15, 56)]
    merged = ledger_manager._get_merged_catchup_txns(existing_txns, new_txns)
    assert ([(i, {}) for i in range(1, 11)] +
            [(i, {}) for i in range(15, 56)] +
            [(i, {}) for i in range(61, 95)]) == merged
