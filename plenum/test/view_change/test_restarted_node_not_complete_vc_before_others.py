from plenum.test.delayers import vcd_delay
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change, start_stopped_node
from stp_core.loop.eventually import eventually

nodeCount = 6


def complete_propagate_primary(node, expected_view_no):
    assert node.view_changer.last_completed_view_no == expected_view_no
    # assert node.view_changer._is_propagated_view_change_completed


def get_last_completed_view_no(nodes):
    completed_view_nos = set([node.view_changer.last_completed_view_no for node in nodes])
    assert len(completed_view_nos) == 1
    return completed_view_nos.pop()


def test_restarted_node_complete_vc_by_current_state(looper,
                                                     txnPoolNodeSet,
                                                     tconf,
                                                     tdir,
                                                     allPluginsPath):
    node_to_restart = txnPoolNodeSet[-1]
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_restart,
                                            stopNode=True)
    looper.removeProdable(node_to_restart)
    old_completed_view_no = get_last_completed_view_no(txnPoolNodeSet[:-1])
    ensure_view_change(looper,
                       txnPoolNodeSet[:-1])
    ensureElectionsDone(looper, txnPoolNodeSet[:-1], customTimeout=tconf.VIEW_CHANGE_TIMEOUT)
    current_completed_view_no = get_last_completed_view_no(txnPoolNodeSet[:-1])
    assert current_completed_view_no > old_completed_view_no

    # Delay VIEW_CHANGE_DONE messages for all nodes
    for node in txnPoolNodeSet[:-1]:
        node.nodeIbStasher.delay(vcd_delay(1000))
    ensure_view_change(looper, txnPoolNodeSet[:-1])

    # Start stopped node until other nodes do view_change
    node_to_restart = start_stopped_node(node_to_restart,
                                         looper,
                                         tconf,
                                         tdir,
                                         allPluginsPath)
    node_to_restart.nodeIbStasher.delay(vcd_delay(1000))
    # check, that restarted node use last completed view no from pool, instead of proposed
    looper.run(eventually(complete_propagate_primary,
                          node_to_restart,
                          current_completed_view_no,
                          timeout=tconf.VIEW_CHANGE_TIMEOUT))
