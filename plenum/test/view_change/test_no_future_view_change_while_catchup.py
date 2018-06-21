import pytest

from plenum.common.startable import Mode
from plenum.test.delayers import icDelay
from plenum.test.helper import checkViewNoForNodes, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone, checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.waits import expectedPoolViewChangeStartedTimeout


@pytest.fixture(params=['starting', 'discovering', 'discovered', 'syncing'])
def mode(request):
    if request.param == 'starting':
        return Mode.starting
    elif request.param == 'discovering':
        return Mode.discovering
    elif request.param == 'discovered':
        return Mode.discovered
    elif request.param == 'syncing':
        return Mode.syncing


def test_no_propagated_future_view_change_until_synced(txnPoolNodeSet, looper, mode):
    # the last node is a lagging one, which will receive ViewChangeDone messages for future view
    viewNo = checkViewNoForNodes(txnPoolNodeSet)
    lagged_node_index = (viewNo + 3) % len(txnPoolNodeSet)
    lagged_node = txnPoolNodeSet[lagged_node_index]
    other_nodes = list(set(txnPoolNodeSet) - {lagged_node})

    # emulate catchup by setting non-synced status
    lagged_node.mode = mode
    old_view_no = checkViewNoForNodes([lagged_node])

    # delay INSTANCE CHANGE on lagged nodes, so all nodes except the lagging one finish View Change
    with delay_rules(lagged_node.nodeIbStasher, icDelay()):
        # make sure that View Change happened on all nodes but the lagging one
        ensure_view_change(looper, other_nodes)
        checkProtocolInstanceSetup(looper=looper, nodes=other_nodes, numInstances=2)
        ensure_all_nodes_have_same_data(looper, nodes=other_nodes)
        looper.runFor(expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet)))
        assert old_view_no == checkViewNoForNodes([lagged_node])

        # emulate finishing of catchup by setting Participating status
        lagged_node.mode = Mode.participating

        # make sure that View Change happened on lagging node
        waitForViewChange(looper, [lagged_node], expectedViewNo=old_view_no + 1,
                          customTimeout=10)
        ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
