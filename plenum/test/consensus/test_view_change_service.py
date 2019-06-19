from plenum.common.messages.node_messages import ViewChange, ViewChangeAck


def test_start_view_change_increases_next_view_and_broadcasts_view_change_message(
        initial_view_no, any_3pc_state, mock_network, view_change_service):
    view_change_service.start_view_change()

    assert any_3pc_state.view_no == initial_view_no + 1
    assert any_3pc_state.waiting_for_new_view
    assert len(mock_network.sent_messages) == 1

    msg, dst = mock_network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, ViewChange)
    assert msg.viewNo == initial_view_no + 1
    assert msg.stableCheckpoint == any_3pc_state.stable_checkpoint


def test_view_change_message_is_responded_with_view_change_ack_to_new_primary(
        any_3pc_state, mock_network, view_change_service):
    view_change_service.start_view_change()
    vc, _ = mock_network.sent_messages.pop()

    mock_network.recv(vc, 'some_node')

    assert len(mock_network.sent_messages) == 1
    msg, dst = mock_network.sent_messages[0]
    assert dst == any_3pc_state.primary_name
    assert isinstance(msg, ViewChangeAck)
    assert msg.viewNo == vc.viewNo
    assert msg.name == any_3pc_state.name
    assert msg.digest == 'digest_of_view_change_message'
