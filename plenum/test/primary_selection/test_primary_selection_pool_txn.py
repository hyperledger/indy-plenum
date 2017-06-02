import pytest

from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet
from plenum.test.pool_transactions.helper import add_2_nodes
from plenum.test.primary_selection.helper import check_newly_added_nodes


@pytest.fixture(scope="module")
def one_node_added(looper, txnPoolNodeSet, nodeThetaAdded):
    # New node knows primary same primary as others and has rank greater
    # than others
    _, _, new_node = nodeThetaAdded
    check_newly_added_nodes(looper, txnPoolNodeSet, [new_node])
    return new_node


def test_primary_selection_non_genesis_node(one_node_added):
    return one_node_added


def test_primary_selection_increase_f(one_node_added, looper, txnPoolNodeSet,
                                      steward1, stewardWallet,
                                      tdirWithPoolTxns, tconf, allPluginsPath):
    new_nodes = add_2_nodes(looper, txnPoolNodeSet, steward1, stewardWallet,
                            tdirWithPoolTxns, tconf, allPluginsPath)

    check_newly_added_nodes(looper, txnPoolNodeSet, new_nodes)

# TODO: Add more tests to make one next primary crashed, malicious, ensure primary
    # selection happens after catchup