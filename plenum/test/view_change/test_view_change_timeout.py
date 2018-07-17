import pytest

from plenum.test.delayers import vcd_delay
from plenum.test.stasher import delay_rules
from plenum.test.helper import waitForViewChange, perf_monitor_disabled, view_change_timeout
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import get_count, getAllReturnVals
from plenum.test.test_node import get_master_primary_node, \
    ensureElectionsDone
from stp_core.loop.eventually import eventually

nodeCount = 7
VIEW_CHANGE_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, VIEW_CHANGE_TIMEOUT), \
         perf_monitor_disabled(tconf):
        yield tconf


def _check_view_change_completed_count(node):
    return get_count(node, node._check_view_change_completed)


def _check_view_change_completed_true(node):
    return len(getAllReturnVals(node, node._check_view_change_completed, compare_val_to=True))


def _check_view_change_completed_stats(nodes):
    return {node.name: (_check_view_change_completed_count(node), _check_view_change_completed_true(node))
            for node in nodes}


def check_watchdog_called_expected_times(nodes, stats, times):
    def call_count(node):
        return _check_view_change_completed_count(node) - stats[node.name][0]

    def true_count(node):
        return _check_view_change_completed_true(node) - stats[node.name][1]

    n = nodeCount
    f = (n - 1) // 3

    call_counts = [call_count(node) for node in nodes]
    true_counts = [true_count(node) for node in nodes]

    ok = True
    ok = ok and all(v <= times for v in call_counts)
    ok = ok and all(v <= times for v in true_counts)
    ok = ok and sum(call_counts) >= times * (n - f)
    ok = ok and sum(true_counts) >= times * (n - f)

    if not ok:
        actual = ""
        for node in nodes:
            actual += "{}: called {}, returned true {}\n".format(node.name, call_count(node), true_count(node))
        raise AssertionError("Watchdog expected to be called {} times, actual counts:\n{}".format(times, actual))


def stop_next_primary(nodes):
    m_next_primary_name = nodes[0]._elector._next_primary_node_name_for_master()
    next(node for node in nodes if node.name == m_next_primary_name).stop()
    alive_nodes = list(filter(lambda x: x.name != m_next_primary_name, nodes))
    return alive_nodes


def start_view_change(nodes, next_view_no):
    for n in nodes:
        n.view_changer.startViewChange(next_view_no)


@pytest.fixture()
def setup(txnPoolNodeSet, looper):
    m_primary_node = get_master_primary_node(list(txnPoolNodeSet))
    initial_view_no = waitForViewChange(looper, txnPoolNodeSet)
    timeout_callback_stats = _check_view_change_completed_stats(txnPoolNodeSet)
    return m_primary_node, initial_view_no, timeout_callback_stats


def test_view_change_retry_by_timeout(
        txnPoolNodeSet, looper, tconf, setup, sdk_pool_handle, sdk_wallet_client):
    """
    Verifies that a view change is restarted if it is not completed in time
    """
    m_primary_node, initial_view_no, timeout_callback_stats = setup
    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    with delay_rules(stashers, vcd_delay()):
        start_view_change(txnPoolNodeSet, initial_view_no + 1)

        # First view change should fail, because of delayed ViewChangeDone
        # messages. This then leads to new view change that we need.
        with pytest.raises(AssertionError):
            ensureElectionsDone(looper=looper,
                                nodes=txnPoolNodeSet,
                                customTimeout=1.5 * VIEW_CHANGE_TIMEOUT)

    # Now as ViewChangeDone messages are unblocked view changes should finish successfully
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    new_m_primary_node = get_master_primary_node(list(txnPoolNodeSet))
    assert m_primary_node.name != new_m_primary_node.name

    # The timeout method was called one time
    check_watchdog_called_expected_times(txnPoolNodeSet, timeout_callback_stats, 1)

    # 2 view changes have been initiated
    for node in txnPoolNodeSet:
        assert node.viewNo - initial_view_no == 2

    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_client,
                               sdk_pool_handle)


def test_multiple_view_change_retries_by_timeouts(
        txnPoolNodeSet, looper, tconf, setup,
        sdk_pool_handle, sdk_wallet_client):
    """
    Verifies that a view change is restarted each time
    when the previous one is timed out
    """
    _, initial_view_no, timeout_callback_stats = setup
    stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    with delay_rules(stashers, vcd_delay()):
        start_view_change(txnPoolNodeSet, initial_view_no + 1)

        # Wait until timeout callback is called 3 times
        looper.run(eventually(check_watchdog_called_expected_times,
                              txnPoolNodeSet, timeout_callback_stats, 3,
                              retryWait=1,
                              timeout=3 * VIEW_CHANGE_TIMEOUT + 2))

        # View changes should fail
        with pytest.raises(AssertionError):
            ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet, customTimeout=1)

    # This view change must be completed with no problems
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # 4 view changes must have been initiated (initial one + 3 retries)
    for node in txnPoolNodeSet:
        assert node.viewNo - initial_view_no == 4

    sdk_ensure_pool_functional(looper, txnPoolNodeSet,
                               sdk_wallet_client,
                               sdk_pool_handle)


def test_view_change_restarted_by_timeout_if_next_primary_disconnected(
        txnPoolNodeSet, looper, tconf, setup):
    """
    Verifies that a view change is restarted by timeout
    if the next primary has been disconnected
    """
    _, initial_view_no, timeout_callback_stats = setup

    start_view_change(txnPoolNodeSet, initial_view_no + 1)

    alive_nodes = stop_next_primary(txnPoolNodeSet)

    ensureElectionsDone(looper=looper, nodes=alive_nodes, numInstances=3)

    # There were 2 view changes
    for node in alive_nodes:
        assert (node.viewNo - initial_view_no) == 2

    # The timeout method was called 1 time
    check_watchdog_called_expected_times(txnPoolNodeSet, timeout_callback_stats, 1)
