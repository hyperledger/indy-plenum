import pytest

from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import add_2_nodes
from plenum.test.primary_selection.helper import check_newly_added_nodes


def ensure_pool_functional(looper, nodes, wallet, client, num_reqs=10,
                           num_batches=2):
    send_reqs_batches_and_get_suff_replies(looper, wallet, client, num_reqs,
                                           num_batches)
    ensure_all_nodes_have_same_data(looper, nodes)


def check_accepted_view_change_sent(node, nodes):
    for other_node in nodes:
        if node == other_node:
            continue
        if other_node.name in node.elector._view_change_done:
            assert node.elector._view_change_done[other_node.name] == \
                node.elector._accepted_view_change_done_message


def test_primary_selection_non_genesis_node(one_node_added, looper,
                                            txnPoolNodeSet, stewardWallet,
                                            steward1):
    ensure_pool_functional(looper, txnPoolNodeSet, stewardWallet, steward1)


@pytest.fixture(scope='module')
def two_more_nodes_added(one_node_added, looper, txnPoolNodeSet,
                         stewardWallet, steward1,
                         tdirWithPoolTxns, tconf, allPluginsPath):
    # check_accepted_view_change_sent(one_node_added, txnPoolNodeSet)

    new_nodes = add_2_nodes(looper, txnPoolNodeSet, steward1, stewardWallet,
                            tdirWithPoolTxns, tconf, allPluginsPath)

    check_newly_added_nodes(looper, txnPoolNodeSet, new_nodes)
    return new_nodes


def test_primary_selection_increase_f(
        two_more_nodes_added,
        looper,
        txnPoolNodeSet,
        stewardWallet,
        steward1):
    # for n in two_more_nodes_added:
    #     check_accepted_view_change_sent(n, txnPoolNodeSet)
    ensure_pool_functional(looper, txnPoolNodeSet, stewardWallet, steward1)


# TODO: Add more tests to make one next primary crashed, malicious, ensure primary
    # selection happens after catchup
