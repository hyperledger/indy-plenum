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
    msg = ViewChangeDone(proposed_view_no, frm, fake_node.ledger_summary, frm=frm)
    res = Node.msgHasAcceptableViewNo(fake_node, msg)
    assert proposed_view_no in fake_node.msgsForFutureViews
    # TODO INDY-1983 seens wrong since key in msgsToViewChanger is a deque of messages
    assert proposed_view_no not in fake_node.msgsToViewChanger
    assert not res
