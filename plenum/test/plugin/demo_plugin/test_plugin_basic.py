from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID


def test_plugin_setup(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert AUCTION_LEDGER_ID in node.ledger_ids
        assert AUCTION_LEDGER_ID in node.ledgerManager.ledgerRegistry
        assert node.ledger_ids == node.ledgerManager.ledger_sync_order
        assert AUCTION_LEDGER_ID in node.states
