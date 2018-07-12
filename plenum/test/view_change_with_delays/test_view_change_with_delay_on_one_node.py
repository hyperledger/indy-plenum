import pytest

from plenum.test.helper import perf_monitor_disabled
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change_with_delays.helper import \
    do_view_change_with_delay_on_one_node

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


@pytest.mark.skip(reason='INDY-1303. Case 4 (simplified): the slow node gets '
                         'a different merkle tree root hash')
def test_view_change_with_delay_on_one_node(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client, tconf):
    """
    Perform view change on one slow node later than on the other nodes so that
    delayed Commits are processed by the slow node in the old view and by the
    other nodes in the new view. After that verify that all the nodes have the
    same ledgers and state.
    """
    do_view_change_with_delay_on_one_node(txnPoolNodeSet[-1], txnPoolNodeSet, looper,
                                          sdk_pool_handle, sdk_wallet_client)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
