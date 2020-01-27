import pytest

from plenum.common.constants import LEDGER_STATUS
from plenum.test.delayers import msg_rep_delay
from plenum.test.helper import checkViewNoForNodes, waitForViewChange, sdk_send_random_and_check, view_change_timeout
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.stasher import delay_rules
from plenum.test.test_node import get_master_primary_node
from plenum.test.view_change.helper import start_stopped_node, ensure_view_change_by_primary_restart, \
    node_sent_instance_changes_count

TestRunningTimeLimitSec = 150


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, 20):
        yield tconf


def test_view_change_after_back_to_quorum_with_disconnected_primary(txnPoolNodeSet, looper,
                                                                    sdk_pool_handle,
                                                                    sdk_wallet_client,
                                                                    tdir, tconf, allPluginsPath):
    assert len(txnPoolNodeSet) == 4

    pr_node = get_master_primary_node(txnPoolNodeSet)
    assert pr_node.name == "Alpha"

    # 1. Initiate view change be primary (Alpha) restart
    nodes = ensure_view_change_by_primary_restart(looper,
                                                  txnPoolNodeSet,
                                                  tconf,
                                                  tdir,
                                                  allPluginsPath,
                                                  customTimeout=2 * tconf.NEW_VIEW_TIMEOUT,
                                                  exclude_from_check=['check_last_ordered_3pc_backup'])

    # Now primary should be Beta
    pr_node = get_master_primary_node(nodes)
    assert pr_node.name == "Beta"

    # 2. Stop non-primary node Delta, no any view changes are expected
    non_primary_to_stop = [n for n in nodes if n.name == "Delta"][0]
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, non_primary_to_stop)
    looper.removeProdable(non_primary_to_stop)

    remaining_nodes = list(set(nodes) - {non_primary_to_stop})
    # Primary is going to be stopped, remember instance change messages count
    # to ensure that no view change happened as number of connected nodes is less
    # than quorum.
    ic_cnt = {}
    for n in remaining_nodes:
        ic_cnt[n.name] = node_sent_instance_changes_count(n)

    # 3. Disconnect primary
    disconnect_node_and_ensure_disconnected(
        looper, remaining_nodes, pr_node)
    looper.removeProdable(pr_node)

    # Wait for more than ToleratePrimaryDisconnection timeout and check that no IC messages presented.
    looper.runFor(tconf.ToleratePrimaryDisconnection + 5)
    remaining_nodes = list(set(remaining_nodes) - {pr_node})
    for n in remaining_nodes:
        assert ic_cnt[n.name] == node_sent_instance_changes_count(n)

    view_no = checkViewNoForNodes(remaining_nodes)

    # 4. Start Delta (non-primary), now primary (Beta) is disconnected but there is a quorum
    # to choose a new one.
    restartedNode = start_stopped_node(non_primary_to_stop, looper, tconf,
                                       tdir, allPluginsPath,
                                       delay_instance_change_msgs=False,
                                       start=False)
    # 5. delay catchup on Delta for more that INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT,
    # so that Delta proposes INSTANCE_CHANGE from view=0 to view=1
    # (it doesn't yet know that the current view is 1, since it hasn't yet finished catchup)
    with delay_rules(restartedNode.nodeIbStasher, msg_rep_delay(types_to_delay=[LEDGER_STATUS])):
        looper.add(restartedNode)
        looper.runFor(tconf.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT + 5)

    remaining_nodes = remaining_nodes + [restartedNode]

    # 6. Check that view change happened eventually because
    # Delta re-send InstanceChang for view=2 after it finished catchup
    waitForViewChange(looper, remaining_nodes, expectedViewNo=(view_no + 1),
                      customTimeout=3 * tconf.NEW_VIEW_TIMEOUT)

    # 7. ensure pool is working properly
    sdk_send_random_and_check(looper, remaining_nodes, sdk_pool_handle,
                              sdk_wallet_client, 3)
    ensure_all_nodes_have_same_data(looper, nodes=remaining_nodes)
