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


"""
Not sure what is happening with this test. Locally it is almost always green (saw it fail just a couple of times).
On Jenkins it fails regularly with the following error:
def checkViewNoForNodes(nodes: Iterable[TestNode], expectedViewNo: int = None):
        viewNos = set()
        for node in nodes:
            logger.debug("{}'s view no is {}".format(node, node.master_replica.viewNo))
            viewNos.add(node.master_replica.viewNo)
        assert len(viewNos) == 1, 'Expected 1, but got {}. ' \
>                                 'ViewNos: {}'.format(len(viewNos), [(n.name, n.master_replica.viewNo) for n in nodes])
E       AssertionError: Expected 1, but got 2. ViewNos: [('Alpha', 1), ('Beta', 1), ('Gamma', 0), ('Delta', 0)]

expectedViewNo = 1
node       = Delta
nodes      = [Alpha, Beta, Gamma, Delta]
viewNos    = {0, 1}
"""
@pytest.mark.skip(reason="Maybe will be fixed by: INDY-2238 (Persist 3PC messages during Ordering)")
def test_view_change_with_delayed_commits_on_multiple_nodes_and_restarts_of_those_nodes(txnPoolNodeSet, looper,
                                                                                        sdk_pool_handle,
                                                                                        sdk_wallet_client, tconf, tdir,
                                                                                        allPluginsPath):
    """
    Delay transaction on half of the pool
    Restart that half
    Trigger View Change
    Check that everything is ok
    """

    slow_nodes = txnPoolNodeSet[-2:]
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
