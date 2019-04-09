import pytest
from plenum.test.freshness.helper import get_all_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_all_ledgers, check_freshness_updated_for_all

from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected

from plenum.test.helper import assertExp, sdk_send_random_and_check, freshness
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 30


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
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
    # Wait for the first freshness update
    looper.run(eventually(
        check_freshness_updated_for_all, rest_nodes,
        timeout=FRESHNESS_TIMEOUT + 5)
    )

    # Wait for the second freshness update
    bls_multi_sigs_after_first_update = get_all_multi_sig_values_for_all_nodes(rest_nodes)
    looper.run(eventually(check_updated_bls_multi_sig_for_all_ledgers,
                          rest_nodes, bls_multi_sigs_after_first_update, FRESHNESS_TIMEOUT,
                          timeout=FRESHNESS_TIMEOUT + 5
                          ))

    # Restart Delta and wait for successful catch up
    restarted_node = start_stopped_node(restarted_node,
                                        looper,
                                        tconf,
                                        tdir,
                                        allPluginsPath,
                                        start=True)

    txnPoolNodeSet[-1] = restarted_node
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, *txnPoolNodeSet)
    assert all(n.viewNo == view_no for n in txnPoolNodeSet)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet)
