from plenum.common.messages.node_messages import ViewChange, ViewChangeAck


def test_start_view_change_increases_next_view_and_broadcasts_view_change_message(
        initial_view_no, consensus_data, mock_network, view_change_service):
    view_change_service.start_view_change()

    assert consensus_data.view_no == initial_view_no + 1
    assert consensus_data.waiting_for_new_view
    assert len(mock_network.sent_messages) == 1

    msg, dst = mock_network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, ViewChange)
    assert msg.viewNo == initial_view_no + 1
    assert msg.stableCheckpoint == consensus_data.stable_checkpoint


def test_view_change_message_is_responded_with_view_change_ack_to_new_primary(
        consensus_data, mock_network, view_change_service):
    view_change_service.start_view_change()
    vc, _ = mock_network.sent_messages.pop()

    mock_network.process_incoming(vc, 'some_node')

    assert len(mock_network.sent_messages) == 1
    msg, dst = mock_network.sent_messages[0]
    assert dst == consensus_data.primary_name
    assert isinstance(msg, ViewChangeAck)
    assert msg.viewNo == vc.viewNo
    assert msg.name == consensus_data.name
    assert msg.digest == 'digest_of_view_change_message'
