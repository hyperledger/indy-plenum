import pytest

from plenum.test.helper import perf_monitor_disabled
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
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


def test_view_change_with_delayed_commits(txnPoolNodeSet, looper,
                                          sdk_pool_handle,
                                          sdk_wallet_client,
                                          tconf):
    # Perform view change with Delta acting as fast node
    # With current view change implementation its state will become different from other nodes
    do_view_change_with_pending_request_and_one_fast_node(txnPoolNodeSet[3], txnPoolNodeSet,
                                                          looper, sdk_pool_handle, sdk_wallet_client)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
