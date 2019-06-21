from plenum.common.event_bus import InternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.helper import MockNetwork


def test_start_view_change_increases_next_view_and_broadcasts_view_change_message(
        initial_view_no, some_consensus_data):
    service = ViewChangeService(some_consensus_data, InternalBus(), MockNetwork())
    service.start_view_change()

    assert service._data.view_no == initial_view_no + 1
    assert service._data.waiting_for_new_view
    assert len(service._network.sent_messages) == 1

    msg, dst = service._network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, ViewChange)
    assert msg.viewNo == initial_view_no + 1
    assert msg.stableCheckpoint == service._data.stable_checkpoint


def test_view_change_message_is_responded_with_view_change_ack_to_new_primary(
        some_consensus_data, other_consensus_data):
    other_service = ViewChangeService(other_consensus_data, InternalBus(), MockNetwork())
    other_service.start_view_change()
    vc, _ = other_service._network.sent_messages[0]

    service = ViewChangeService(some_consensus_data, InternalBus(), MockNetwork())
    service.start_view_change()
    service._network.sent_messages.clear()

    service._network.process_incoming(vc, other_service._data.name)

    # TODO: Need to somehow extract fixtures primary_consensus_data and
    #  non_primary_consensus_data to properly separate tests
    if not service._data.is_primary():
        assert len(service._network.sent_messages) == 1
        msg, dst = service._network.sent_messages[0]
        assert dst == service._data.primary_name()
        assert isinstance(msg, ViewChangeAck)
        assert msg.viewNo == vc.viewNo
        assert msg.name == other_service._data.name
        assert msg.digest == 'digest_of_view_change_message'
    else:
        assert not service._network.sent_messages
