from plenum.test.helper import send_reqs_batches_and_get_suff_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import add_2_nodes
from plenum.test.primary_selection.helper import check_newly_added_nodes


def ensure_pool_functional(looper, nodes, wallet, client, num_reqs=10,
                           num_batches=2):
    send_reqs_batches_and_get_suff_replies(looper, wallet, client, 10, 2)
    ensure_all_nodes_have_same_data(looper, nodes)


def test_primary_selection_non_genesis_node(one_node_added, looper,
                                            txnPoolNodeSet, stewardWallet,
                                            steward1):
    ensure_pool_functional(looper, txnPoolNodeSet, stewardWallet, steward1)


def test_primary_selection_increase_f(one_node_added, looper, txnPoolNodeSet,
                                      stewardWallet, steward1,
                                      tdirWithPoolTxns, tconf, allPluginsPath):
    new_nodes = add_2_nodes(looper, txnPoolNodeSet, steward1, stewardWallet,
                            tdirWithPoolTxns, tconf, allPluginsPath)

    check_newly_added_nodes(looper, txnPoolNodeSet, new_nodes)
    ensure_pool_functional(looper, txnPoolNodeSet, stewardWallet, steward1)


# TODO: Add more tests to make one next primary crashed, malicious, ensure primary
    # selection happens after catchup