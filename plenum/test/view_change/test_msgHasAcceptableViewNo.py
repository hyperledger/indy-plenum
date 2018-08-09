import pytest
from plenum.common.messages.node_messages import ViewChangeDone
from plenum.server.node import Node

@pytest.fixture(scope='function', params=[False, True])
def view_change_in_progress(request):
    return request.param


def test_future_vcdone_vc(fake_node, view_change_in_progress):
    """
    If from_current_state is False, then message should be put only into msgsForFutureViews queue
    """
    frm = 'Node3'
    fake_node.view_changer.view_change_in_progress = view_change_in_progress
    current_view = fake_node.viewNo
    proposed_view_no = current_view + 1
    msg = ViewChangeDone(proposed_view_no, frm, fake_node.ledger_summary)
    res = Node.msgHasAcceptableViewNo(fake_node, msg, frm)
    assert proposed_view_no in fake_node.msgsForFutureViews
    assert proposed_view_no not in fake_node.msgsToViewChanger
    assert not res


def test_from_current_state(fake_node):
    """
    If from_current_state is True and is initial propagate primary (current viewNo is 0),
    then message should be put into msgsToViewChanger queue with is_initial_propagate_primary flag as True
    """
    frm = 'Node3'
    fake_node.view_changer.view_change_in_progress = False
    current_view = fake_node.view_changer.last_completed_view_no
    proposed_view_no = current_view + 1
    msg = ViewChangeDone(proposed_view_no, frm, fake_node.ledger_summary)
    res = Node.msgHasAcceptableViewNo(fake_node, msg, frm, from_current_state=True)
    msg, frm = fake_node.msgsToViewChanger[0]
    assert len(fake_node.msgsToViewChanger) == 1
    assert msg.is_initial_propagate_primary
    assert res is False
