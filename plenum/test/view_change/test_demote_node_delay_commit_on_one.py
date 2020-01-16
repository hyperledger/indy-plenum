from plenum.test.delayers import cDelay
from plenum.test.helper import waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import demote_node
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone

nodeCount = 7


def test_demote_node_delay_commit_on_one(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo
    slow_node = txnPoolNodeSet[-2]

    # Demote Node7 but don't allow Node6 to be aware of it.
    with delay_rules(slow_node.nodeIbStasher, cDelay()):
        demote_node(looper, sdk_wallet_stewards[-1], sdk_pool_handle,
                    txnPoolNodeSet[-1])
        del txnPoolNodeSet[-1]

        waitForViewChange(looper, txnPoolNodeSet, view_no + 1)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
