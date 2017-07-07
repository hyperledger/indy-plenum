import pytest
from plenum.test.delayers import reset_delays_and_process_delayeds, vcd_delay
from plenum.test.helper import waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional
from plenum.test.spy_helpers import get_count, getAllReturnVals
from plenum.test.test_node import get_master_primary_node, \
    ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7


def test_view_change_timeout(nodeSet, looper, up, wallet1, client1):
    """
    Check view change restarted if it is not completed in time
    """

    m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    initial_view_no = waitForViewChange(looper, nodeSet)
    # Setting view change timeout to low value to make test pass quicker
    for node in nodeSet:
        node._view_change_timeout = 5

    # Delaying view change messages to make first view change fail
    # due to timeout
    for node in nodeSet:
        node.nodeIbStasher.delay(vcd_delay(delay=50))

    # Delaying preprepae messages from nodes and
    # sending request to force view change
    #for i in range(3):
    #    delayNonPrimaries(nodeSet, instId=i, delay=10)
    #sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    times = {}
    for node in nodeSet:
        times[node.name] = {
            'called': get_count(node, node._check_view_change_completed),
            'returned_true': len(getAllReturnVals(
                node, node._check_view_change_completed, compare_val_to=True))
        }

    for node in nodeSet:
        node.startViewChange(initial_view_no + 1)

    # First view change should fail, because of delayed
    # instance change messages. This then leads to new view change that we need.
    with pytest.raises(AssertionError):
        ensureElectionsDone(looper=looper, nodes=nodeSet, customTimeout=10)

    # Resetting delays to let second view change go well
    reset_delays_and_process_delayeds(nodeSet)

    # This view change should be completed with no problems
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)
    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name

    # The timeout method has been called at least once
    for node in nodeSet:
        assert get_count(node, node._check_view_change_completed) > times[node.name]['called']
        assert len(getAllReturnVals(node,
                                    node._check_view_change_completed,
                                    compare_val_to=True)) > times[node.name]['returned_true']

    # Multiple view changes have been initiated
    for node in nodeSet:
        assert (node.viewNo - initial_view_no) > 1

    ensure_pool_functional(looper, nodeSet, wallet1, client1)
