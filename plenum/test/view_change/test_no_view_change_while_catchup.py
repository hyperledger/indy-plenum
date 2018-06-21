from plenum.common.startable import Mode
from plenum.test.delayers import icDelay
from plenum.test.helper import checkViewNoForNodes, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone, checkEveryNodeHasAtMostOnePrimary
from plenum.test.view_change.helper import do_view_change, revert_do_view_change, ensure_view_change


def test_no_view_change_while_syncing(txnPoolNodeSet, looper):
    # emulate catchup by setting Syncing status
    for node in txnPoolNodeSet:
        node.mode = Mode.syncing

    # start View Change
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    old_meths = do_view_change(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        node.view_changer.sendInstanceChange(old_view_no + 1)
    looper.runFor(5)

    # make sure View Change is not started
    assert old_view_no == checkViewNoForNodes(txnPoolNodeSet)

    # emulate finishing of catchup by setting Participating status
    revert_do_view_change(txnPoolNodeSet, old_meths)
    for node in txnPoolNodeSet:
        node.mode = Mode.participating

    # make sure that View Change happened
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=old_view_no + 1)


def test_no_propagated_future_view_change_while_syncing(txnPoolNodeSet, looper):
    # the last node is a lagging one, which will receive ViewChangeDone messages for future view
    lagged_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]

    # emulate catchup by setting Syncing status
    lagged_node.mode = Mode.syncing
    old_view_no = checkViewNoForNodes([lagged_node])

    # delay INSTANCE CHANGE on lagged nodes, so all nodes except the lagging one finish View Change
    with delay_rules(lagged_node.nodeIbStasher, icDelay()):
        # make sure that View Change happened on all nodes but the lagging one
        ensure_view_change(looper, other_nodes)
        checkEveryNodeHasAtMostOnePrimary(looper=looper, nodes=other_nodes)
        ensure_all_nodes_have_same_data(looper, nodes=other_nodes)
        looper.runFor(5)
        assert old_view_no == checkViewNoForNodes([lagged_node])

        # emulate finishing of catchup by setting Participating status
        lagged_node.mode = Mode.participating

        # make sure that View Change happened on lagging node
        waitForViewChange(looper, [lagged_node], expectedViewNo=old_view_no + 1,
                          customTimeout=10)
