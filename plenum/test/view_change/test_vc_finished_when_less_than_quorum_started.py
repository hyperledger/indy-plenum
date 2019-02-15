import pytest

from plenum.test.helper import sdk_send_random_and_check, assertExp, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import restart_node, nodes_received_ic
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_val = tconf.ToleratePrimaryDisconnection
    tconf.ToleratePrimaryDisconnection = 1000
    yield tconf
    tconf.ToleratePrimaryDisconnection = old_val


def test_vc_finished_when_less_than_quorum_started(looper, txnPoolNodeSet,
                                                   sdk_wallet_client, sdk_pool_handle,
                                                   tconf, tdir, allPluginsPath):

    alpha, beta, gamma, delta = txnPoolNodeSet

    # Delta and Gamma send InstanceChange for all nodes.
    for node in [gamma, delta]:
        node.view_changer.on_master_degradation()
        looper.run(
            eventually(nodes_received_ic, txnPoolNodeSet, node, 1))

    # Restart Alpha, Beta, Gamma
    for node in [alpha, beta, gamma]:
        restart_node(looper, txnPoolNodeSet, node, tconf, tdir, allPluginsPath)
    alpha, beta, gamma, delta = txnPoolNodeSet

    # Send InstanceChange from Beta for all nodes
    beta.view_changer.on_master_degradation()

    # Ensure that pool is still functional
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)

    # Alpha and Gamma send InstanceChange for all nodes.
    for node in [gamma, alpha]:
        node.view_changer.on_master_degradation()

    ensureElectionsDone(looper, txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=1,
                      customTimeout=tconf.VIEW_CHANGE_TIMEOUT)

    # Ensure that pool is still functional
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
