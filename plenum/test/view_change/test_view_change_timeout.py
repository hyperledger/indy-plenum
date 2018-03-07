import pytest
from plenum.test.delayers import reset_delays_and_process_delayeds, vcd_delay
from plenum.test.helper import waitForViewChange, stopNodes
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional
from plenum.test.spy_helpers import get_count, getAllReturnVals
from plenum.test.test_node import get_master_primary_node, \
    ensureElectionsDone
from stp_core.loop.eventually import eventually

nodeCount = 7
view_change_timeout = 5


@pytest.fixture()
def setup(nodeSet, looper):
    m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    initial_view_no = waitForViewChange(looper, nodeSet)
    # Setting view change timeout to low value to make test pass quicker
    for node in nodeSet:
        node._view_change_timeout = view_change_timeout

    timeout_callback_stats = {}
    for node in nodeSet:
        timeout_callback_stats[node.name] = {
            'called': get_count(node, node._check_view_change_completed),
            'returned_true': len(getAllReturnVals(
                node, node._check_view_change_completed, compare_val_to=True))
        }
    return m_primary_node, initial_view_no, timeout_callback_stats


def test_view_change_retry_by_timeout(
        nodeSet, looper, up, setup, wallet1, client1):
    """
    Verifies that a view change is restarted if it is not completed in time
    """
    m_primary_node, initial_view_no, timeout_callback_stats = setup

    delay_view_change_done_msg(nodeSet)

    start_view_change(nodeSet, initial_view_no + 1)
    # First view change should fail, because of delayed ViewChangeDone
    # messages. This then leads to new view change that we need.
    with pytest.raises(AssertionError):
        ensureElectionsDone(looper=looper,
                            nodes=nodeSet,
                            customTimeout=view_change_timeout + 2)

    # Resetting delays to let second view change go well
    reset_delays_and_process_delayeds(nodeSet)

    # This view change should be completed with no problems
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)
    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name

    # The timeout method was called one time
    for node in nodeSet:
        assert get_count(node,
                         node._check_view_change_completed) - \
               timeout_callback_stats[node.name]['called'] == 1
        assert len(getAllReturnVals(node,
                                    node._check_view_change_completed,
                                    compare_val_to=True)) - \
               timeout_callback_stats[node.name]['returned_true'] == 1

    # 2 view changes have been initiated
    for node in nodeSet:
        assert node.viewNo - initial_view_no == 2

    ensure_pool_functional(looper, nodeSet, wallet1, client1)


def test_multiple_view_change_retries_by_timeouts(
        nodeSet, looper, up, setup, wallet1, client1):
    """
    Verifies that a view change is restarted each time
    when the previous one is timed out
    """
    _, initial_view_no, timeout_callback_stats = setup

    delay_view_change_done_msg(nodeSet)

    start_view_change(nodeSet, initial_view_no + 1)

    def check_timeout_callback_called(times):
        for node in nodeSet:
            assert get_count(node,
                             node._check_view_change_completed) - \
                   timeout_callback_stats[node.name]['called'] == times
            assert len(getAllReturnVals(node,
                                        node._check_view_change_completed,
                                        compare_val_to=True)) - \
                   timeout_callback_stats[node.name]['returned_true'] == times

    # Check that the timeout method was called 3 times
    # during the triple view_change_timeout period plus a margin
    looper.run(eventually(check_timeout_callback_called, 3,
                          retryWait=1,
                          timeout=3 * view_change_timeout + 2))

    # Check that the last view change has failed
    with pytest.raises(AssertionError):
        ensureElectionsDone(looper=looper, nodes=nodeSet, customTimeout=1)

    # Reset delays to let the next view change go well
    reset_delays_and_process_delayeds(nodeSet)

    # This view change must be completed with no problems
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

    # 4 view changes must have been initiated (initial one + 3 retries)
    for node in nodeSet:
        assert node.viewNo - initial_view_no == 4

    ensure_pool_functional(looper, nodeSet, wallet1, client1)


def test_view_change_restarted_by_timeout_if_next_primary_disconnected(
        nodeSet, looper, up, setup, wallet1, client1):
    """
    Verifies that a view change is restarted by timeout
    if the next primary has been disconnected
    """
    _, initial_view_no, timeout_callback_stats = setup

    start_view_change(nodeSet, initial_view_no + 1)

    alive_nodes = stop_next_primary(nodeSet)

    ensureElectionsDone(looper=looper, nodes=alive_nodes, numInstances=3)

    # There were 2 view changes
    for node in alive_nodes:
        assert (node.viewNo - initial_view_no) == 2

    # The timeout method was called 1 time
    for node in alive_nodes:
        assert get_count(node,
                         node._check_view_change_completed) - \
               timeout_callback_stats[node.name]['called'] == 1
        assert len(getAllReturnVals(node,
                                    node._check_view_change_completed,
                                    compare_val_to=True)) - \
               timeout_callback_stats[node.name]['returned_true'] == 1


def stop_next_primary(nodes):
    m_next_primary_name = nodes[0]._elector._next_primary_node_name_for_master()
    nodes[m_next_primary_name].stop()
    alive_nodes = list(filter(lambda x: x.name != m_next_primary_name, nodes))
    return alive_nodes


def delay_view_change_done_msg(nodes):
    for node in nodes:
        node.nodeIbStasher.delay(vcd_delay(delay=50))


def start_view_change(nodes, next_view_no):
    for node in nodes:
        node.view_changer.startViewChange(next_view_no)
