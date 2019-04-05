import pytest

from plenum.common.messages.node_messages import FutureViewChangeDone
from plenum.common.startable import Mode
from plenum.test.delayers import icDelay
from plenum.test.helper import checkViewNoForNodes, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone, checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.waits import expectedPoolViewChangeStartedTimeout
from stp_core.loop.eventually import eventually


@pytest.fixture(params=[Mode.starting, Mode.discovering, Mode.discovered, Mode.syncing])
def mode(request):
    return request.param


def check_future_vcd_count(node, expected_count):
    vcd_count = sum(1 for msg in node.view_changer.inBox if isinstance(msg, FutureViewChangeDone))
    assert expected_count == vcd_count


def try_view_change(looper, node):
    looper.run(eventually(node.view_changer.serviceQueues))


def check_no_view_change(looper, node):
    looper.run(eventually(check_future_vcd_count, node, 3,
                          timeout=expectedPoolViewChangeStartedTimeout(4)))
    try_view_change(looper, node)
    check_future_vcd_count(node, 3)


def test_no_propagated_future_view_change_until_synced(txnPoolNodeSet, looper, mode):
    # the last node is a lagging one, which will receive ViewChangeDone messages for future view
    viewNo = checkViewNoForNodes(txnPoolNodeSet)
    lagged_node_index = (viewNo + 3) % len(txnPoolNodeSet)
    lagged_node = txnPoolNodeSet[lagged_node_index]
    other_nodes = list(set(txnPoolNodeSet) - {lagged_node})

    # emulate catchup by setting non-synced status
    lagged_node.mode = mode
    old_view_no = checkViewNoForNodes([lagged_node])

    check_future_vcd_count(lagged_node, 0)

    # delay INSTANCE CHANGE on lagged nodes, so all nodes except the lagging one finish View Change
    with delay_rules(lagged_node.nodeIbStasher, icDelay()):
        # make sure that View Change happened on all nodes but the lagging one
        ensure_view_change(looper, other_nodes)
        checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, instances=range(2))
        ensure_all_nodes_have_same_data(looper, nodes=other_nodes)

        check_no_view_change(looper, lagged_node)
        assert old_view_no == checkViewNoForNodes([lagged_node])

        # emulate finishing of catchup by setting Participating status
        lagged_node.mode = Mode.participating

        # make sure that View Change happened on lagging node
        waitForViewChange(looper, [lagged_node], expectedViewNo=old_view_no + 1,
                          customTimeout=10)
        ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
