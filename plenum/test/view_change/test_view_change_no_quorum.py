import pytest

from plenum.test.view_change.helper import ensure_view_change
from plenum.test.helper import stopNodes


def test_view_change_no_quorum(nodeSet, up, looper):
    """
    Check that less than n - f votes are not sufficient for view change
    """

    assert len(nodeSet) == 4

    # Quorum for view change is expected to be n - f
    # So, switching two node off
    all_nodes = list(nodeSet)
    stopped = all_nodes[-2:]
    active = all_nodes[:-1]
    stopNodes(stopped, looper)
    looper.removeProdable(*stopped)

    # Check that view does not changes
    current_view_no = all_nodes[0].viewNo
    with pytest.raises(AssertionError,
                       message="{} == {}, "
                               "Alpha -> Ratio: None,"
                               "Beta -> Ratio: None,"
                               "Delta -> Ratio: None"
                               .format(current_view_no,
                                       current_view_no + 1)) as exc_info:
        ensure_view_change(looper, active)
