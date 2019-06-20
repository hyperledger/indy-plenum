from plenum.common.constants import AUDIT_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID
from plenum.server.node import Node


def test_ledger_sync_order(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        check_ledger_sync_order(node)


def check_ledger_sync_order(node: Node):
    leecher = node.ledgerManager._node_leecher

    # TODO: It does ignore designated ledger order, however it will obsolete after implemnting parallel catchup
    ledger_ids = [ledger.id for ledger in node.ledgerManager.ledgerRegistry.values()
                  if ledger.id not in [AUDIT_LEDGER_ID, POOL_LEDGER_ID, CONFIG_LEDGER_ID]]

    for prev, next in zip(ledger_ids, ledger_ids[1:]):
        assert leecher._get_next_ledger(prev) == next

    assert leecher._get_next_ledger(None) == ledger_ids[0]
    assert leecher._get_next_ledger(ledger_ids[-1]) == None
