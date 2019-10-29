import pytest

from plenum.test.helper import sdk_send_random_and_check, perf_monitor_disabled
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change_with_delays.helper import \
    do_view_change_with_propagate_primary_on_one_delayed_node

# This is needed only with current view change implementation to give enough time
# to show what is exactly broken
TestRunningTimeLimitSec = 300


@pytest.fixture(scope="module")
def tconf(tconf):
    """
    Patch config so that monitor won't start view change unexpectedly
    """
    with perf_monitor_disabled(tconf):
        yield tconf


def test_two_view_changes_with_propagate_primary_on_one_delayed_node(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client, tconf):
    """
    Perform two view changes in such a way that a view change is performed on
    all the nodes except for one slow node and then propagate primary is
    performed on it so that delayed Commits are processed by the slow node in
    the old view and by the other nodes in the new view (the slow nodes for the
    two view changes are different). After that verify that a new request can
    be ordered.
    """
    do_view_change_with_propagate_primary_on_one_delayed_node(
        txnPoolNodeSet[-1], txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client)

    do_view_change_with_propagate_primary_on_one_delayed_node(
        txnPoolNodeSet[0], txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
