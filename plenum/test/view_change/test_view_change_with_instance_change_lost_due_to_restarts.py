import pytest

from plenum.test.helper import freshness, waitForViewChange
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.restart.helper import restart_nodes
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import send_test_instance_change
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 20


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_view_change_with_instance_change_lost_due_to_restarts(looper, txnPoolNodeSet,
                                                               sdk_pool_handle,
                                                               sdk_wallet_client,
                                                               tconf, tdir, allPluginsPath):
    """
    1. some_nodes (Beta and Gamma) send InstanceChange for all nodes.
    2. Restart other_nodes (Gamma and Delta)
    3. last_node (Delta) send InstanceChange for all nodes.
    4. Ensure elections done and pool is functional
    """
    current_view_no = txnPoolNodeSet[0].viewNo
    some_nodes = txnPoolNodeSet[1:3]
    other_nodes = txnPoolNodeSet[2:4]

    for n in some_nodes:
        send_test_instance_change(n)

    def check_ic_delivery():
        for node in txnPoolNodeSet:
            vct_service = node.master_replica._view_change_trigger_service
            assert all(vct_service._instance_changes.
                       has_inst_chng_from(current_view_no + 1, sender.name)
                       for sender in some_nodes)
    looper.run(eventually(check_ic_delivery))

    restart_nodes(looper, txnPoolNodeSet,
                  other_nodes, tconf, tdir, allPluginsPath,
                  start_one_by_one=False)

    last_node = txnPoolNodeSet[-1]
    send_test_instance_change(last_node)
    waitForViewChange(looper, txnPoolNodeSet, current_view_no + 1, customTimeout=3 * FRESHNESS_TIMEOUT)

    ensureElectionsDone(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
