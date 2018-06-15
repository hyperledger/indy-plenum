from plenum.common.startable import Mode
from plenum.test.delayers import icDelay
from plenum.test.helper import checkViewNoForNodes, waitForViewChange
from plenum.test.stasher import delay_rules
from plenum.test.view_change.helper import do_view_change, revert_do_view_change, ensure_view_change


def test_no_view_change_while_syncing(txnPoolNodeSet, looper):
    for node in txnPoolNodeSet:
        node.mode = Mode.syncing

    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    old_meths = do_view_change(txnPoolNodeSet)
    for node in txnPoolNodeSet:
        node.view_changer.sendInstanceChange(old_view_no + 1)
    looper.runFor(5)
    new_view_no = checkViewNoForNodes(txnPoolNodeSet)

    assert old_view_no == new_view_no

    revert_do_view_change(txnPoolNodeSet, old_meths)
    for node in txnPoolNodeSet:
        node.mode = Mode.participating

    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=old_view_no + 1)


def test_no_propagated_future_view_change_while_syncing(txnPoolNodeSet, looper):
    lagged_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]
    lagged_node.mode = Mode.syncing
    old_view_no = checkViewNoForNodes([lagged_node])

    with delay_rules(lagged_node.nodeIbStasher, icDelay()):
        ensure_view_change(looper, other_nodes)
        looper.runFor(5)
        assert old_view_no == checkViewNoForNodes([lagged_node])

        lagged_node.mode = Mode.participating
        waitForViewChange(looper, [lagged_node], expectedViewNo=old_view_no + 1,
                          customTimeout=10)
