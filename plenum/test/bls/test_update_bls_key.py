import pytest
from plenum.common.constants import ALIAS, BLS_KEY
from plenum.common.keygen_utils import init_bls_keys
from plenum.test.bls.helper import check_bls_multi_sig_after_send
from plenum.test.node_catchup.helper import waitNodeDataEquality, ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.pool_transactions.helper import updateNodeData

nodeCount = 4
nodes_wth_bls = 4


@pytest.fixture(scope='module')
def pool_no_bls():
    pass


def _change_bls_key(looper, txnPoolNodeSet, tdirWithPoolTxns,
                    node,
                    steward_client, steward_wallet):
    new_blspk = init_bls_keys(tdirWithPoolTxns, node.name)
    node_data = {
        ALIAS: node.name,
        BLS_KEY: new_blspk
    }

    updateNodeData(looper, steward_client, steward_wallet, node, node_data)
    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1])
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward_client,
                                                  *txnPoolNodeSet)
    return new_blspk


def _check_bls_key(blskey, node, nodes):
    # check that each node has correct blskey for this node
    for n in nodes:
        assert blskey == n.bls_bft.bls_key_register.get_key_by_name(node.name)

    # check that this node has correct blskey
    assert blskey == node.bls_bft.bls_crypto_signer.pk


def check_update_bls_key(node_num, saved_multi_sigs_count,
                         looper, txnPoolNodeSet, tdirWithPoolTxns,
                         client, wallet,
                         stewards_and_wallets):
    node = txnPoolNodeSet[node_num]
    steward_client, steward_wallet = stewards_and_wallets[node_num]

    new_blspk = _change_bls_key(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                node,
                                steward_client, steward_wallet)
    _check_bls_key(new_blspk, node, txnPoolNodeSet)

    check_bls_multi_sig_after_send(looper, txnPoolNodeSet,
                                   client, wallet,
                                   saved_multi_sigs_count=saved_multi_sigs_count)


def test_update_bls_one_node(looper, txnPoolNodeSet, tdirWithPoolTxns,
                             client1, wallet1,
                             stewards_and_wallets):
    check_update_bls_key(0, 0,
                         looper, txnPoolNodeSet, tdirWithPoolTxns,
                         client1, wallet1,
                         stewards_and_wallets
                         )


def test_update_bls_two_nodes():
    pass


def test_update_bls_all_nodes():
    pass
