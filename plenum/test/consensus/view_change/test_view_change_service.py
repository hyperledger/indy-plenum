import pytest

from plenum.common.event_bus import InternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.server.consensus.view_change_service import ViewChangeService, view_change_digest
from plenum.test.helper import MockNetwork


@pytest.fixture
def view_change_service(consensus_data, mock_timer):
    def _service(name):
        data = consensus_data(name)
        service = ViewChangeService(data, mock_timer, InternalBus(), MockNetwork())
        return service

    return _service


@pytest.fixture
def view_change_acks(validators, random):
    def _view_change_acks(vc, vc_frm, primary, count):
        digest = view_change_digest(vc)
        non_senders = [name for name in validators if name not in [vc_frm, primary]]
        ack_frms = random.sample(non_senders, count)
        return [(ViewChangeAck(viewNo=vc.viewNo, name=vc_frm, digest=digest), ack_frm) for ack_frm in ack_frms]

    return _view_change_acks


def test_view_change_primary_selection(validators, initial_view_no):
    primary = ViewChangeService._find_primary(validators, initial_view_no)
    prev_primary = ViewChangeService._find_primary(validators, initial_view_no - 1)
    next_primary = ViewChangeService._find_primary(validators, initial_view_no + 1)

    assert primary in validators
    assert prev_primary in validators
    assert next_primary in validators

    assert primary != prev_primary
    assert primary != next_primary


def test_start_view_change_increases_next_view_changes_primary_and_broadcasts_view_change_message(
        some_item, validators, view_change_service, initial_view_no):
    service = view_change_service(some_item(validators))
    old_primary = service._data.primary_name

    service.start_view_change()

    assert service._data.view_no == initial_view_no + 1
    assert service._data.waiting_for_new_view
    assert service._data.primary_name != old_primary

    assert len(service._network.sent_messages) == 1

    msg, dst = service._network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, ViewChange)
    assert msg.viewNo == initial_view_no + 1
    assert msg.stableCheckpoint == service._data.stable_checkpoint


def test_non_primary_responds_to_view_change_message_with_view_change_ack_to_new_primary(
        some_item, other_item, validators, primary, view_change_service, initial_view_no, view_change_message):
    next_view_no = initial_view_no + 1
    non_primary_name = some_item(validators, exclude=[primary(next_view_no)])
    service = view_change_service(non_primary_name)
    service.start_view_change()
    service._network.sent_messages.clear()

    vc = view_change_message(next_view_no)
    frm = other_item(validators, exclude=[non_primary_name])
    service._network.process_incoming(vc, frm)

    assert len(service._network.sent_messages) == 1
    msg, dst = service._network.sent_messages[0]
    assert dst == service._data.primary_name
    assert isinstance(msg, ViewChangeAck)
    assert msg.viewNo == vc.viewNo
    assert msg.name == frm
    assert msg.digest == view_change_digest(vc)


def test_primary_doesnt_respond_to_view_change_message(
        some_item, validators, primary, view_change_service, initial_view_no, view_change_message):
    name = primary(initial_view_no + 1)
    service = view_change_service(name)

    vc = view_change_message(initial_view_no + 1)
    frm = some_item(validators, exclude=[name])
    service._network.process_incoming(vc, frm)

    assert len(service._network.sent_messages) == 0


def test_new_view_message_is_sent_once_when_view_change_certificate_is_reached(
        validators, primary, view_change_service, initial_view_no, view_change_message, view_change_acks):
    primary_name = primary(initial_view_no + 1)
    service = view_change_service(primary_name)
    service.start_view_change()
    service._network.sent_messages.clear()

    non_primaries = [item for item in validators if item != primary_name]
    for vc_frm in non_primaries:
        vc = view_change_message(initial_view_no + 1)
        service._network.process_incoming(vc, vc_frm)

        for ack, ack_frm in view_change_acks(vc, vc_frm, primary_name, len(validators) - 2):
            service._network.process_incoming(ack, ack_frm)

    assert len(service._network.sent_messages) == 1
    msg, dst = service._network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, NewView)
    assert msg.viewNo == initial_view_no + 1
