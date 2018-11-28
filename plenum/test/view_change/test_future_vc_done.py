from plenum.common.messages.node_messages import ViewChangeDone


def test_future_vcdone_when_propagate_primary_no_quorum(fake_node):
    """
    Check, that view_change would not be started without quorum of future_view_change_done messages
    if propagate_primary is True
    """
    frm = 'Node3'
    current_view = fake_node.view_changer.last_completed_view_no
    proposed_view_no = current_view + 1
    msg = ViewChangeDone(proposed_view_no, frm, fake_node.ledger_summary)
    fake_node.view_changer._next_view_indications[proposed_view_no] = {}
    fake_node.view_changer._next_view_indications[proposed_view_no][frm] = msg
    view_changer = fake_node.view_changer
    res = view_changer._start_view_change_if_possible(proposed_view_no,
                                                      propagate_primary=True)
    assert res is False


def test_future_vcdone_when_propagate_primary_with_quorum(fake_node):
    """
    Check, that view_change would be started with quorum of future_view_change_done messages
    if propagate_primary is True
    """
    quorum = fake_node.f + 1
    frms = fake_node.allNodeNames[-quorum:]
    current_view = fake_node.view_changer.last_completed_view_no
    proposed_view_no = current_view + 1
    msgs = [ViewChangeDone(proposed_view_no, frm, fake_node.ledger_summary) for frm in frms]
    fake_node.view_changer._next_view_indications[proposed_view_no] = {}
    for i in range(len(msgs)):
        fake_node.view_changer._next_view_indications[proposed_view_no][frms[i]] = msgs[i]
    view_changer = fake_node.view_changer
    res = view_changer._start_view_change_if_possible(proposed_view_no,
                                                      propagate_primary=True)
    assert res is True


def test_future_vcdone_now_when_propagate_primary_no_quorum(fake_node):
    """
    Check, that view_change would not be started without quorum of future_view_change_done messages
    if propagate_primary is False
    """
    quorum = fake_node.f + 1
    frms = fake_node.allNodeNames[-quorum:]
    current_view = fake_node.view_changer.last_completed_view_no
    proposed_view_no = current_view + 1
    msgs = [ViewChangeDone(proposed_view_no, frm, fake_node.ledger_summary) for frm in frms]
    fake_node.view_changer._next_view_indications[proposed_view_no] = {}
    for i in range(len(msgs)):
        fake_node.view_changer._next_view_indications[proposed_view_no][frms[i]] = msgs[i]
    view_changer = fake_node.view_changer
    res = view_changer._start_view_change_if_possible(proposed_view_no,
                                                      propagate_primary=False)
    assert res is False


def test_future_vcdone_now_when_propagate_primary_with_quorum(fake_node):
    """
    Check, that view_change would be started without quorum of future_view_change_done messages
    if propagate_primary is False
    """
    quorum = fake_node.totalNodes - fake_node.f
    frms = fake_node.allNodeNames[-quorum:]
    current_view = fake_node.view_changer.last_completed_view_no
    proposed_view_no = current_view + 1
    msgs = [ViewChangeDone(proposed_view_no, frm, fake_node.ledger_summary) for frm in frms]
    fake_node.view_changer._next_view_indications[proposed_view_no] = {}
    for i in range(len(msgs)):
        fake_node.view_changer._next_view_indications[proposed_view_no][frms[i]] = msgs[i]
    view_changer = fake_node.view_changer
    res = view_changer._start_view_change_if_possible(proposed_view_no,
                                                      propagate_primary=False)
    assert res is True
