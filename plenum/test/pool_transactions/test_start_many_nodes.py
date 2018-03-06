# Start with 8 nodes
from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.common.constants import NYM, ROLE, STEWARD
from plenum.common.constants import TXN_TYPE
from plenum.test.helper import assertEquality

nodeCount = 8


def test_genesis_nodes(looper, txnPoolNodeSet,
                       sdk_pool_handle,
                       sdk_wallet_client):
    assert len(txnPoolNodeSet) == nodeCount
    for node in txnPoolNodeSet:
        assertEquality(node.poolLedger.size, nodeCount)
        stw_count = sum(1 for _, txn in node.domainLedger.getAllTxn() if
                        (txn[TXN_TYPE] == NYM) and (txn.get(ROLE) == STEWARD))
        assertEquality(stw_count, nodeCount)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
