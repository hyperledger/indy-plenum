from plenum.common.messages.node_messages import InstanceChange


def test_instance_change_from_known(view_change_trigger_service):
    current_view = view_change_trigger_service._data.view_no
    proposed_view = current_view + 1
    ic_msg = InstanceChange(viewNo=proposed_view, reason=26)
    frm = list(view_change_trigger_service._data.validators)[0]
    view_change_trigger_service.process_instance_change(ic_msg, frm=frm)
    assert view_change_trigger_service._instance_changes.has_inst_chng_from(proposed_view, frm)


def test_instance_change_from_unknown(view_change_trigger_service):
    current_view = view_change_trigger_service._data.view_no
    proposed_view = current_view + 1
    ic_msg = InstanceChange(viewNo=proposed_view, reason=26)
    frm = 'SomeUnknownNode'
    view_change_trigger_service.process_instance_change(ic_msg, frm=frm)
    assert not view_change_trigger_service._instance_changes.has_inst_chng_from(proposed_view, frm)
