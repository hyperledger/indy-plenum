from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change, start_stopped_node

nodeCount = 6


def test_vc_by_current_state(txnPoolNodeSet,
                             looper,
                             tdir,
                             tconf,
                             allPluginsPath):
    node_to_stop = txnPoolNodeSet[-1]
    old_view_no = node_to_stop.view_changer.last_completed_view_no
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_stop,
                                            stopNode=True)
    looper.removeProdable(node_to_stop)
    ensure_view_change(looper, txnPoolNodeSet[:-1])
    ensureElectionsDone(looper,
                        txnPoolNodeSet[:-1],
                        customTimeout=tconf.VIEW_CHANGE_TIMEOUT)
    new_view_no = txnPoolNodeSet[0].view_changer.last_completed_view_no
    assert new_view_no > old_view_no
    node_to_stop = start_stopped_node(node_to_stop,
                                      looper,
                                      tconf,
                                      tdir,
                                      allPluginsPath)
    txnPoolNodeSet[-1] = node_to_stop
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=tconf.VIEW_CHANGE_TIMEOUT)
    assert node_to_stop.view_changer.last_completed_view_no == new_view_no
