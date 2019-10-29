import pytest

from plenum.test.helper import sdk_send_random_and_check, perf_monitor_disabled
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change_with_delays.helper import do_view_change_with_pending_request_and_one_fast_node

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


def test_two_view_changes_with_delayed_commits(txnPoolNodeSet, looper,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               tconf):
    # Perform view change with Delta acting as fast node
    # With current view change implementation its state will become different from other nodes
    do_view_change_with_pending_request_and_one_fast_node(txnPoolNodeSet[3], txnPoolNodeSet,
                                                          looper, sdk_pool_handle, sdk_wallet_client)

    # Perform view change with Alpha acting as fast node
    # With current view change implementation its state will become different from other nodes,
    # resulting in pool losing consensus and failing to finish view change at all
    do_view_change_with_pending_request_and_one_fast_node(txnPoolNodeSet[0], txnPoolNodeSet,
                                                          looper, sdk_pool_handle, sdk_wallet_client)

    # Check that pool can write transactions
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
