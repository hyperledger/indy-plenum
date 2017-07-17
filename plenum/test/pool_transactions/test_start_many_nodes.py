# Start with 8 nodes
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional

nodeCount = 8

def test_steward_add_multiple_nodes(looper, txnPoolNodeSet,
                                         client1, wallet1, client1Connected):
    assert len(txnPoolNodeSet) == nodeCount
    ensure_pool_functional(looper, txnPoolNodeSet, wallet1, client1)
