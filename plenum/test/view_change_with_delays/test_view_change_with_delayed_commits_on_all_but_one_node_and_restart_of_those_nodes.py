import pytest

from plenum.test.helper import perf_monitor_disabled
from plenum.test.view_change_with_delays.helper import do_view_change_with_delayed_commits_and_node_restarts


@pytest.fixture(scope="module")
def tconf(tconf):
    """
    Patch config so that monitor won't start view change unexpectedly
    """
    with perf_monitor_disabled(tconf):
        yield tconf


@pytest.mark.skip(reason="Should be fixed by: INDY-2238 (Persist 3PC messages during Ordering)")
def test_view_change_with_delayed_commits_on_all_but_one_node_and_restart_of_those_nodes(txnPoolNodeSet, looper,
                                                                                         sdk_pool_handle,
                                                                                         sdk_wallet_client, tconf, tdir,
                                                                                         allPluginsPath):
    """
    Order transactions on only one node
    Restart all of the other nodes
    Trigger View Change
    Check that everything is ok
    """

    slow_nodes = txnPoolNodeSet[1:]
    fast_nodes = [node for node in txnPoolNodeSet if node not in slow_nodes]

    do_view_change_with_delayed_commits_and_node_restarts(
        fast_nodes=fast_nodes,
        slow_nodes=slow_nodes,
        nodes_to_restart=slow_nodes,
        old_view_no=slow_nodes[0].viewNo,
        old_last_ordered=slow_nodes[0].master_replica.last_ordered_3pc,
        looper=looper,
        sdk_pool_handle=sdk_pool_handle,
        sdk_wallet_client=sdk_wallet_client,
        tconf=tconf,
        tdir=tdir,
        all_plugins_path=allPluginsPath,
    )
