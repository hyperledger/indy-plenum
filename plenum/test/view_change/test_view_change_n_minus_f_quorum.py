import pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.helper import stopNodes


def test_view_change_n_minus_f_quorum(txnPoolNodeSet, looper):
    """
    Check that quorum n - f is used for view change
    """

    assert len(txnPoolNodeSet) == 4

    # Quorum for view change is expected to be n - f
    # So, switching one node off
    stopped = [txnPoolNodeSet[-1]]
    active = list(txnPoolNodeSet)[:-1]
    stopNodes(stopped, looper)
    looper.removeProdable(*stopped)

    # Check that view changes
    ensure_view_change(looper, active)
    ensureElectionsDone(looper=looper, nodes=active,
                        instances_list=range(2), customTimeout=60)
    ensure_all_nodes_have_same_data(looper, nodes=active)

    # Switching another node off to make sure that this time the quorum is not
    # reaches.
    stopped = [active[-1]]
    active = list(active)[:-1]
    stopNodes(stopped, looper)
    looper.removeProdable(*stopped)

    # Check that view does not changes
    current_view_no = active[0].viewNo
    with pytest.raises(AssertionError,
                       message="{} == {}, "
                               "Alpha -> Ratio: None,"
                               "Beta -> Ratio: None,"
                               "Delta -> Ratio: None"
                               .format(current_view_no,
                                       current_view_no + 1)) as exc_info:
        ensure_view_change(looper, active)
