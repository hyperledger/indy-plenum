from plenum.common.messages.node_messages import ViewChange, ViewChangeAck


def test_start_view_change_increases_next_view_and_broadcasts_view_change_message(
        initial_view_no, some_consensus_data, some_node_network, some_view_change_service):
    some_view_change_service.start_view_change()

    assert some_consensus_data.view_no == initial_view_no + 1
    assert some_consensus_data.waiting_for_new_view
    assert len(some_node_network.sent_messages) == 1

    msg, dst = some_node_network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, ViewChange)
    assert msg.viewNo == initial_view_no + 1
    assert msg.stableCheckpoint == some_consensus_data.stable_checkpoint


def test_view_change_message_is_responded_with_view_change_ack_to_new_primary(
        some_consensus_data, some_node_network, some_view_change_service,
        other_node_name, other_node_network, other_view_change_service):
    other_view_change_service.start_view_change()
    vc, _ = other_node_network.sent_messages.pop()

    some_view_change_service.start_view_change()
    some_node_network.process_incoming(vc, other_node_name)

    assert len(some_node_network.sent_messages) == 2
    msg, dst = some_node_network.sent_messages[1]
    assert dst == some_consensus_data.primary_name()
    assert isinstance(msg, ViewChangeAck)
    assert msg.viewNo == vc.viewNo
    assert msg.name == other_node_name
    assert msg.digest == 'digest_of_view_change_message'
