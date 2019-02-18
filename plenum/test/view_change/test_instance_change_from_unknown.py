def test_instance_change_from_known(fake_view_changer):
    current_view = fake_view_changer.node.viewNo
    proposed_view = current_view + 1
    ic_msg = fake_view_changer._create_instance_change_msg(view_no=proposed_view,
                                                           suspicion_code=26)
    frm = list(fake_view_changer.node.nodestack.connecteds)[0]
    fake_view_changer.process_instance_change_msg(ic_msg,
                                                  frm=frm)
    assert fake_view_changer.instance_changes.has_inst_chng_from(proposed_view, frm)


def test_instance_change_from_unknown(fake_view_changer):
    current_view = fake_view_changer.node.viewNo
    proposed_view = current_view + 1
    ic_msg = fake_view_changer._create_instance_change_msg(view_no=proposed_view,
                                                           suspicion_code=26)
    frm = b'SomeUnknownNode'
    fake_view_changer.process_instance_change_msg(ic_msg,
                                                  frm=frm)
    assert not fake_view_changer.instance_changes.has_inst_chng_from(proposed_view, frm)
