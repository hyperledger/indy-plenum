# Start with 8 nodes
from plenum.common.constants import NYM, ROLE, STEWARD
from plenum.common.constants import TXN_TYPE
from plenum.test.helper import assertEquality
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional

nodeCount = 8


def test_genesis_nodes(looper, txnPoolNodeSet,
                       client1, wallet1, client1Connected):
    assert len(txnPoolNodeSet) == nodeCount
    for node in txnPoolNodeSet:
        assertEquality(node.poolLedger.size, nodeCount)
        stw_count = sum(1 for _, txn in node.domainLedger.getAllTxn() if
                        (txn[TXN_TYPE] == NYM) and (txn.get(ROLE) == STEWARD))
        assertEquality(stw_count, nodeCount)
    ensure_pool_functional(looper, txnPoolNodeSet, wallet1, client1)
