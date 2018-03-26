def test_ledger_sync_order(looper, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        ledger_ids = node.ledger_ids
        for idx, lid in enumerate(ledger_ids):
            next_ledger_id = node.ledgerManager.ledger_to_sync_after(lid)
            if idx != (len(ledger_ids) - 1):
                assert next_ledger_id == ledger_ids[idx + 1]
            else:
                assert next_ledger_id is None
