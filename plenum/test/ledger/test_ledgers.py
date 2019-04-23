from plenum.common.constants import POOL_LEDGER_ID, AUDIT_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID


def test_default_ledgers(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert node.poolLedger is not None
        assert node.domainLedger is not None
        assert node.configLedger is not None
        assert node.auditLedger is not None


def test_default_ledgers_order(txnPoolNodeSet):
    expected_ledger_ids = [AUDIT_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]
    for node in txnPoolNodeSet:
        assert node.ledger_ids == expected_ledger_ids
        for ledger_id in expected_ledger_ids:
            assert node.ledgerManager.ledgerRegistry[ledger_id]

