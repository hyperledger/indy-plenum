from functools import partial
from unittest.mock import patch

from plenum.common.constants import VIEW_CHANGE_DONE
from plenum.common.messages.node_messages import ViewChangeStartMessage
from plenum.common.types import f
from plenum.server.view_change.pre_view_change_strategies import VCStartMsgStrategy

from plenum.server.node import Node
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test.delayers import msg_rep_delay
from plenum.test.helper import sdk_send_random_and_check, assertExp, waitForViewChange

from plenum.test import waits
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.restart.helper import get_group, restart_nodes
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkNodesConnected, ensureElectionsDone
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually


def test_restart_node_with_view_changes(tdir, tconf,
                                        looper,
                                        txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_client,
                                        allPluginsPath):
    '''
    1. Stop the node Delta
    2. Patch methods for processing VCStartMsgStrategy messages
    3. Delay CurrentState messages on Delta
    4. Start Delta
    5. Start view change with a maser degradation reason (from view 0 to 1)
    6. Check that Delta start VCStartMsgStrategy after quorum of InstanceChanges
    7. Reset delay for CurrentStates
    8. Check that propagate primary happened.
    9. Unpatch VCStartMsgStrategy methods and process catching messages.
    10. Start view change with a maser degradation reason (from view 1 to 2)
    11. Check that all nodes has viewNo = 2 and can order transactions.
    '''
    # Prepare nodes
    lagging_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]
    start_view_no = lagging_node.viewNo

    # Stop Delta
    waitNodeDataEquality(looper, lagging_node, *rest_nodes)
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            lagging_node,
                                            stopNode=True)
    looper.removeProdable(lagging_node)

    # Send more requests to active nodes
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, len(rest_nodes) * 3)
    waitNodeDataEquality(looper, *rest_nodes)

    # Restart stopped node
    lagging_node = start_stopped_node(lagging_node,
                                      looper,
                                      tconf,
                                      tdir,
                                      allPluginsPath,
                                      start=False,
                                      )

    # Add to lagging_node node route a patched method for processing
    # ViewChangeStartMessage to delay processing.
    global view_change_started_messages
    view_change_started_messages = []

    def patch_on_view_change_started(node, msg: ViewChangeStartMessage):
        view_change_started_messages.append((node, msg))

    processor = partial(patch_on_view_change_started,
                        lagging_node)
    lagging_node.nodeMsgRouter.add((ViewChangeStartMessage, processor))

    # Delay CurrentState messages on lagging_node to delay propagate primary
    with delay_rules(lagging_node.nodeIbStasher, msg_rep_delay()):
        # Add lagging_node to pool
        looper.add(lagging_node)
        txnPoolNodeSet[-1] = lagging_node
        looper.run(checkNodesConnected(txnPoolNodeSet))
        looper.run(
            eventually(lambda: assertExp(len(lagging_node.nodeIbStasher.delayeds) >= 3)))

        # Start ViewChange (0 -> 1)
        for n in rest_nodes:
            n.view_changer.on_master_degradation()

        # Lagging node still did not catchup, so it can't participate and process I_CH
        looper.run(
            eventually(
                lambda: assertExp(len(view_change_started_messages) == 0)))

    # Lagging node catches up till old view
    looper.run(
        eventually(
            lambda: assertExp(lagging_node.viewNo == start_view_no)))

    # Unpatch ViewChangeStartMessages processing and process delayed messages
    processor = partial(VCStartMsgStrategy.on_view_change_started,
                        lagging_node)
    lagging_node.nodeMsgRouter.add((ViewChangeStartMessage, processor))
    for (_, msg) in view_change_started_messages:
        # TODO INDY-1983 before lagging_node was passed as frm that seems incorrect
        lagging_node.view_changer.node.nodeInBox.append(msg)

    waitForViewChange(looper,
                      txnPoolNodeSet,
                      expectedViewNo=start_view_no + 1,
                      customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet)))

    # Start ViewChange (1 -> 2)
    for n in rest_nodes:
        n.view_changer.on_master_degradation()
    waitForViewChange(looper,
                      txnPoolNodeSet,
                      expectedViewNo=start_view_no + 2,
                      customTimeout=waits.expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet)))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
                        instances_list=range(txnPoolNodeSet[0].requiredNumberOfInstances))

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet)
