import pytest
from plenum.bls.bls_key_register_pool_manager import BlsKeyRegisterPoolManager
from plenum.common.constants import NODE, TXN_TYPE, BLS_KEY, DATA, ALIAS
from plenum.test.pool_transactions.helper import updateNodeData
from plenum.test.primary_selection.conftest import stewardAndWalletForMasterNode, txnPoolMasterNodes
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected

nodeCount = 4


@pytest.fixture()
def node(txnPoolNodeSet):
    return txnPoolNodeSet[0]


@pytest.fixture()
def pool_node_txns(poolTxnData):
    node_txns = []
    for txn in poolTxnData["txns"]:
        if txn[TXN_TYPE] == NODE:
            node_txns.append(txn)
    return node_txns


@pytest.fixture()
def bls_key_register_ledger(node):
    return BlsKeyRegisterPoolManager(node.poolManager)


def test_current_committed_root(bls_key_register_ledger, node):
    committed_root = bls_key_register_ledger.get_pool_root_hash_committed()
    assert committed_root
    assert committed_root == node.poolManager.state.committedHeadHash


def test_get_key_for_current_root(bls_key_register_ledger, txnPoolNodeSet, pool_node_txns):
    for i in range(nodeCount):
        bls_key = bls_key_register_ledger.get_key_by_name(
            txnPoolNodeSet[i].name)
        assert bls_key
        assert bls_key == pool_node_txns[i][DATA][BLS_KEY]


def test_get_key_for_current_root_explicitly(bls_key_register_ledger, txnPoolNodeSet, pool_node_txns):
    for i in range(nodeCount):
        bls_key = bls_key_register_ledger.get_key_by_name(txnPoolNodeSet[i].name,
                                                          bls_key_register_ledger.get_pool_root_hash_committed())
        assert bls_key
        assert bls_key == pool_node_txns[i][DATA][BLS_KEY]


def test_get_key_for_old_root_keys_changed(bls_key_register_ledger,
                                           pool_node_txns,
                                           node,
                                           looper,
                                           stewardAndWalletForMasterNode):
    old_bls_key = pool_node_txns[0][DATA][BLS_KEY]
    new_bls_key = pool_node_txns[0][DATA][BLS_KEY] + "Changed"
    old_pool_root_hash = node.poolManager.state.committedHeadHash

    # change BLS keys
    client, wallet = stewardAndWalletForMasterNode
    change_bls_keys(new_bls_key, node,
                    looper, client, wallet)

    new_pool_root_hash = node.poolManager.state.committedHeadHash
    assert old_pool_root_hash != new_pool_root_hash

    # get old and new keys
    bls_key = bls_key_register_ledger.get_key_by_name(node.name,
                                                      old_pool_root_hash)
    assert bls_key
    assert bls_key == old_bls_key

    bls_key = bls_key_register_ledger.get_key_by_name(node.name,
                                                      new_pool_root_hash)
    assert bls_key
    assert bls_key == new_bls_key


def change_bls_keys(new_bls_key, node,
                    looper, client, wallet):
    node_data = {
        ALIAS: node.name,
        BLS_KEY: new_bls_key
    }
    updateNodeData(looper,
                   client,
                   wallet,
                   node,
                   node_data)
    return
