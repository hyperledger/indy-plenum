import pytest
from plenum.common.constants import TARGET_NYM, TXN_TYPE, NODE, ALIAS, SERVICES, CLIENT_STACK_SUFFIX
from plenum.test.pool_transactions.helper import updateNodeData
from plenum.test.primary_selection.conftest import stewardAndWalletForMasterNode, txnPoolMasterNodes


nodeCount = 7
nodes_wth_bls = 0

@pytest.fixture()
def pool_node_txns(poolTxnData):
    node_txns = []
    for txn in poolTxnData["txns"]:
        if txn[TXN_TYPE] == NODE:
            node_txns.append(txn)
    return node_txns

def test_get_nym_by_name(txnPoolNodeSet, pool_node_txns):
    check_get_nym_by_name(txnPoolNodeSet, pool_node_txns)


def test_get_nym_by_name_not_in_registry(txnPoolNodeSet, pool_node_txns):
    nodes_to_remove = [txnPoolNodeSet[4].name, txnPoolNodeSet[5].name]
    for node in txnPoolNodeSet:
        for node_to_remove in nodes_to_remove:
            del node.nodeReg[node_to_remove]
            del node.cliNodeReg[node_to_remove + CLIENT_STACK_SUFFIX]
    check_get_nym_by_name(txnPoolNodeSet, pool_node_txns)


def test_get_nym_by_name_demoted(txnPoolNodeSet, pool_node_txns,
                                 looper, stewardAndWalletForMasterNode):
    client, wallet = stewardAndWalletForMasterNode
    demote_node(txnPoolNodeSet[0], looper, client, wallet)
    check_get_nym_by_name(txnPoolNodeSet, pool_node_txns)


def demote_node(node,
                looper, client, wallet):
    node_data = {
        ALIAS: node.name,
        SERVICES: []
    }
    updateNodeData(looper,
                   client,
                   wallet,
                   node,
                   node_data)


def check_get_nym_by_name(txnPoolNodeSet, pool_node_txns):
    for i in range(nodeCount):
        node = txnPoolNodeSet[i]
        pool_manager = node.poolManager
        node_name = node.name

        node_nym = pool_manager.get_nym_by_name(node_name)
        expected_data = pool_node_txns[i][TARGET_NYM]

        assert node_nym
        assert node_nym == expected_data



