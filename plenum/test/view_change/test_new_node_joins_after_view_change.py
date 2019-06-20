import pytest

from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import ensureElectionsDone, getNonPrimaryReplicas
from plenum.test.view_change.helper import ensure_view_change, start_stopped_node
from stp_core.loop.eventually import eventually

from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.pool_transactions.conftest import sdk_node_theta_added
from plenum.test.primary_selection.conftest import sdk_one_node_added

from stp_core.common.log import getlogger

logger = getlogger()


@pytest.fixture(scope='module')
def all_nodes_view_change(
        looper,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client):
    for _ in range(5):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2)
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


@pytest.fixture(scope='module')
def new_node_in_correct_view(all_nodes_view_change, looper, txnPoolNodeSet,
                             sdk_one_node_added, sdk_pool_handle, sdk_wallet_client):
    new_node = sdk_one_node_added
    looper.run(eventually(checkViewNoForNodes, txnPoolNodeSet, retryWait=1,
                          timeout=10))
    assert len(getAllReturnVals(new_node.view_changer,
                                new_node.view_changer._do_view_change_by_future_vcd,
                                compare_val_to=True)) == 0
    assert not new_node.view_changer._next_view_indications
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 2)


def test_new_node_has_same_view_as_others(new_node_in_correct_view):
    """
    A node joins after view change.
    """


def test_old_non_primary_restart_after_view_change(new_node_in_correct_view,
                                                   looper, txnPoolNodeSet,
                                                   tdir,
                                                   allPluginsPath, tconf,
                                                   sdk_pool_handle,
                                                   sdk_wallet_client):
    """
    An existing non-primary node crashes and then view change happens,
    the crashed node comes back up after view change
    """
    node_to_stop = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    old_view_no = node_to_stop.viewNo

    # Stop non-primary
    disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet,
                                            node_to_stop, stopNode=True)
    looper.removeProdable(node_to_stop)
    remaining_nodes = list(set(txnPoolNodeSet) - {node_to_stop})

    # Send some requests before view change
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 5)
    ensure_view_change(looper, remaining_nodes, custom_timeout=tconf.VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper, remaining_nodes)
    # Send some requests after view change
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 5)

    restarted_node = start_stopped_node(node_to_stop, looper, tconf,
                                        tdir, allPluginsPath)
    txnPoolNodeSet = remaining_nodes + [restarted_node]
    looper.run(eventually(checkViewNoForNodes,
                          txnPoolNodeSet, old_view_no + 1, timeout=30))
    assert len(getAllReturnVals(restarted_node.view_changer,
                                restarted_node.view_changer._do_view_change_by_future_vcd,
                                compare_val_to=True)) == 0

    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)
    assert not restarted_node.view_changer._next_view_indications
