import pytest

from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected

from plenum.test.helper import assertExp, sdk_send_random_and_check, freshness
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=3):
        yield tconf


def test_freshness_after_catchup(looper,
                                 txnPoolNodeSet,
                                 sdk_pool_handle,
                                 sdk_wallet_client,
                                 sdk_wallet_steward,
                                 tconf,
                                 tdir,
                                 allPluginsPath):
    """
    A node restart and restores last ordered with freshness.
    """
    view_no = txnPoolNodeSet[0].viewNo

    restarted_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]

    # Stop Delta
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            restarted_node,
                                            stopNode=True)
    looper.removeProdable(restarted_node)

    # Send more requests to active nodes
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, len(rest_nodes) * 3)
    waitNodeDataEquality(looper, *rest_nodes)

    looper.runFor(tconf.STATE_FRESHNESS_UPDATE_INTERVAL * 2)

    # Restart Delta and wait for successful catch up
    restarted_node = start_stopped_node(restarted_node,
                                        looper,
                                        tconf,
                                        tdir,
                                        allPluginsPath,
                                        start=True)

    txnPoolNodeSet[-1] = restarted_node
    looper.run(checkNodesConnected(txnPoolNodeSet))

    last_ordered = txnPoolNodeSet[-1].master_last_ordered_3PC
    primaries = txnPoolNodeSet[0].primaries
    looper.run(eventually(lambda: assertExp(n.primaries == primaries for n in txnPoolNodeSet)))
    looper.run(eventually(lambda: assertExp(n.viewNo == view_no for n in txnPoolNodeSet)))
    looper.run(eventually(lambda: assertExp(n.master_last_ordered_3PC == last_ordered for n in txnPoolNodeSet)))

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet)
