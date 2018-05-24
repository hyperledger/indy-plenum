import pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.view_change.view_change_with_delays.helper import \
    do_view_change_with_commits_after_view_change_on_one_node

# This is needed only with current view change implementation to give enough time
# to show what is exactly broken
TestRunningTimeLimitSec = 300


@pytest.fixture(scope="module")
def tconf(tconf):
    """
    Patch config so that monitor won't start view change unexpectedly
    """
    old_unsafe = tconf.unsafe
    tconf.unsafe.add("disable_view_change")
    yield tconf
    tconf.unsafe = old_unsafe


@pytest.mark.skip(reason='INDY-1303. Case 4: one node gets a different merkle tree root hash')
def test_view_change_with_commits_after_view_change_on_one_of_nodes(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client, tconf):
    """
    Test view change when commits are received in one view by all the nodes
    except one slow node and in the next view by the slow node.
    """
    do_view_change_with_commits_after_view_change_on_one_node(txnPoolNodeSet[-1], txnPoolNodeSet, looper,
                                                              sdk_pool_handle, sdk_wallet_client)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
