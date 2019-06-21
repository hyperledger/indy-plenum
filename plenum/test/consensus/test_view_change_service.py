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


def test_non_primary_responds_to_view_change_message_with_view_change_ack_to_new_primary(
        initial_view_no, consensus_data_of_non_primary_in_next_view, other_validator):
    service = ViewChangeService(consensus_data_of_non_primary_in_next_view, InternalBus(), MockNetwork())

    vc = ViewChange(
        viewNo=initial_view_no + 1,
        stableCheckpoint=4,
        prepared=[],
        preprepared=[],
        checkpoints=[]
    )
    service._network.process_incoming(vc, other_validator)

    assert len(service._network.sent_messages) == 1
    msg, dst = service._network.sent_messages[0]
    assert dst == service._data.primary_name()
    assert isinstance(msg, ViewChangeAck)
    assert msg.viewNo == vc.viewNo
    assert msg.name == other_validator
    assert msg.digest == 'digest_of_view_change_message'


def test_primary_doesnt_respond_to_view_change_message(
        initial_view_no, consensus_data_of_primary_in_next_view, other_validator):
    service = ViewChangeService(consensus_data_of_primary_in_next_view, InternalBus(), MockNetwork())

    vc = ViewChange(
        viewNo=initial_view_no + 1,
        stableCheckpoint=4,
        prepared=[],
        preprepared=[],
        checkpoints=[]
    )
    service._network.process_incoming(vc, other_validator)
    assert len(service._network.sent_messages) == 0


# def test_new_view_message_is_not_sent_by_non_primary