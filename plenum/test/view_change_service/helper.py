from plenum.common.constants import PREPREPARE
from plenum.test.delayers import cDelay, ppDelay, msg_rep_delay
from plenum.test.helper import waitForViewChange
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import add_new_node


def get_next_primary_name(txnPoolNodeSet, expected_view_no):
    inst_count = len(txnPoolNodeSet[0].replicas)
    return txnPoolNodeSet[0].primaries_selector.select_primaries(expected_view_no)[0]


def trigger_view_change(nodes):
    for node in nodes:
        node.view_changer.on_master_degradation()


def check_view_change_adding_new_node(looper, tdir, tconf, allPluginsPath,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client,
                                      sdk_wallet_steward,
                                      slow_nodes=[],
                                      delay_commit=False,
                                      delay_pre_prepare=False):
    # Pre-requisites: viewNo=3, Primary is Node4
    for viewNo in range(1, 4):
        trigger_view_change(txnPoolNodeSet)
        waitForViewChange(looper, txnPoolNodeSet, viewNo)
        ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)

    # Delay 3PC messages on slow nodes
    fast_nodes = [node for node in txnPoolNodeSet if node not in slow_nodes]
    slow_stashers = [slow_node.nodeIbStasher for slow_node in slow_nodes]
    delayers = []
    if delay_pre_prepare:
        delayers.append(ppDelay())
        delayers.append(msg_rep_delay(types_to_delay=[PREPREPARE]))
    if delay_commit:
        delayers.append(cDelay())

    with delay_rules_without_processing(slow_stashers, *delayers):
        # Add Node5
        new_node = add_new_node(looper,
                                fast_nodes,
                                sdk_pool_handle,
                                sdk_wallet_steward,
                                tdir,
                                tconf,
                                allPluginsPath)
        old_set = list(txnPoolNodeSet)
        txnPoolNodeSet.append(new_node)

        # Trigger view change
        trigger_view_change(txnPoolNodeSet)

        # make sure view change is finished eventually
        waitForViewChange(looper, old_set, 4)
        ensureElectionsDone(looper, old_set)

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
