import pytest as pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import restart_node
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    old_interval = tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL
    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = 5
    yield tconf
    tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL = old_interval


def test_view_change_not_happen_if_ic_is_discarded(looper, txnPoolNodeSet,
                                                   sdk_pool_handle,
                                                   sdk_wallet_client,
                                                   tconf, tdir, allPluginsPath):
    """
    1. panic_node (Delta) send InstanceChange for all nodes.
    2. Restart nodes_to_restart (Beta, Gamma).
    3. Wait OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL sec.
    4. nodes_to_restart send InstanceChanges for all nodes.
    5. View change doesn't happen since old InstanceChange from panic_node was discarded due to timeout.
    5. Ensure elections done
    """
    nodes_to_restart = txnPoolNodeSet[1:3]
    panic_node = txnPoolNodeSet[-1]
    view_no = txnPoolNodeSet[0].viewNo

    panic_node.view_changer.on_master_degradation()
    for n in nodes_to_restart:
        restart_node(looper, txnPoolNodeSet, n, tconf, tdir, allPluginsPath)
    nodes_to_restart = txnPoolNodeSet[1:3]

    # waiting to discard InstanceChange
    def check_old_ic_discarded():
        assert all(not n.view_changer.instance_changes.has_inst_chng_from(view_no + 1, panic_node.name)
                   for n in txnPoolNodeSet)

    looper.run(eventually(check_old_ic_discarded, timeout=tconf.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL + 1))

    for n in nodes_to_restart:
        n.view_changer.on_master_degradation()

    def check_ic():
        for node in txnPoolNodeSet:
            assert all(node.view_changer.instance_changes.has_inst_chng_from(view_no + 1, n.name)
                       for n in nodes_to_restart)

    looper.run(eventually(check_ic))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    for node in txnPoolNodeSet:
        assert node.viewNo == view_no
