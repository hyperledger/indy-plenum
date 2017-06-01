import pytest

from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet
from plenum.test.pool_transactions.helper import add_2_nodes
from plenum.test.primary_selection.helper import \
    check_rank_consistent_across_each_node, check_newly_added_node
from plenum.test.test_node import checkProtocolInstanceSetup


@pytest.fixture(scope="module")
def one_node_added(looper, txnPoolNodeSet, nodeThetaAdded):
    # New node knows primary same primary as others and has rank greater
    # than others
    _, _, new_node = nodeThetaAdded
    check_newly_added_node(looper, txnPoolNodeSet, new_node)
    return new_node


def test_primary_selection_non_genesis_node(one_node_added):
    return one_node_added


def test_primary_selection_increase_f(one_node_added, looper, txnPoolNodeSet,
                                      steward1, stewardWallet,
                                      tdirWithPoolTxns, tconf, allPluginsPath):
    new_nodes = add_2_nodes(looper, txnPoolNodeSet, steward1, stewardWallet,
                            tdirWithPoolTxns, tconf, allPluginsPath)

    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

    for n in [one_node_added] + new_nodes:
        check_newly_added_node(looper, txnPoolNodeSet, n)


# TODO: Add more tests to make one next primary faulty, ensure primary
    # selection happens after catchup