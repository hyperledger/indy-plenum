from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change


def test_set_last_completed_view_no_after_vc_complete(txnPoolNodeSet, looper):
    """
    Check that view change is done when no txns in the ldegr
    """
    old_views = set(n.viewNo for n in txnPoolNodeSet)
    assert len(old_views) == 1
    old_view = old_views.pop()
    assert all([old_view == n.view_changer.last_completed_view_no for n in txnPoolNodeSet])
    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    new_views = set(n.viewNo for n in txnPoolNodeSet)
    assert len(new_views) == 1
    new_view = new_views.pop()
    assert all([new_view == n.view_changer.last_completed_view_no for n in txnPoolNodeSet])
