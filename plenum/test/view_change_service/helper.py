from plenum.common.constants import PREPREPARE
from plenum.common.messages.internal_messages import NodeNeedViewChange
from plenum.test.delayers import cDelay, ppDelay, msg_rep_delay, old_view_pp_reply_delay
from plenum.test.helper import waitForViewChange, checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules_without_processing, delay_rules
from plenum.test.test_node import ensureElectionsDone, getNonPrimaryReplicas
from plenum.test.view_change.helper import add_new_node
from stp_core.loop.eventually import eventually


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


def check_has_commits(nodes):
    for n in nodes:
        assert len(n.master_replica._ordering_service.commits) > 0


def check_view_change_one_slow_node(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                    vc_counts, slow_node_is_next_primary,
                                    delay_commit=True,
                                    delay_pre_prepare=True):
    current_view_no = checkViewNoForNodes(txnPoolNodeSet)
    expected_view_no = current_view_no + vc_counts
    next_primary = get_next_primary_name(txnPoolNodeSet, expected_view_no)
    pretenders = [r.node for r in getNonPrimaryReplicas(txnPoolNodeSet) if not r.isPrimary]
    if slow_node_is_next_primary:
        delayed_node = [n for n in pretenders if n.name == next_primary][0]
    else:
        delayed_node = [n for n in pretenders if n.name != next_primary][0]
    fast_nodes = [node for node in txnPoolNodeSet if node != delayed_node]

    delayers = []
    if delay_pre_prepare:
        delayers.append(ppDelay())
        delayers.append(msg_rep_delay(types_to_delay=[PREPREPARE]))
    if delay_commit:
        delayers.append(cDelay())

    # delay OldViewPrePrepareReply so that slow node doesn't receive PrePrepares before ReOrdering phase finishes
    with delay_rules(delayed_node.nodeIbStasher, old_view_pp_reply_delay()):
        with delay_rules_without_processing(delayed_node.nodeIbStasher, *delayers):
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
            trigger_view_change(txnPoolNodeSet)
            if vc_counts == 2:
                for node in txnPoolNodeSet:
                    node.master_replica.internal_bus.send(NodeNeedViewChange(current_view_no + 2))

        waitForViewChange(looper=looper, txnPoolNodeSet=txnPoolNodeSet, expectedViewNo=expected_view_no)
        ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)

        # wait till fast nodes finish re-ordering
        looper.run(eventually(check_has_commits, fast_nodes))

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
