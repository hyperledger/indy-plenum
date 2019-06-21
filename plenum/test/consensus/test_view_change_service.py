import string

import pytest

from plenum.common.event_bus import InternalBus
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.test.consensus.helper import view_change_message
from plenum.test.helper import MockNetwork


@pytest.fixture
def view_change_service(consensus_data, validators, initial_view_no):
    def _service(name):
        data = consensus_data(name)
        service = ViewChangeService(data, InternalBus(), MockNetwork())
        return service
    return _service


def test_view_change_primary_selection(validators, initial_view_no):
    primary = ViewChangeService._find_primary(validators, initial_view_no)
    prev_primary = ViewChangeService._find_primary(validators, initial_view_no - 1)
    next_primary = ViewChangeService._find_primary(validators, initial_view_no + 1)

    assert primary in validators
    assert prev_primary in validators
    assert next_primary in validators

    assert primary != prev_primary
    assert primary != next_primary


def test_start_view_change_increases_next_view_and_broadcasts_view_change_message(
        some_item, validators, view_change_service, initial_view_no):
    service = view_change_service(some_item(validators))
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
        some_item, other_item, validators, primary, view_change_service, initial_view_no):
    name = some_item(validators, exclude=[primary(initial_view_no + 1)])
    service = view_change_service(name)

    vc = ViewChange(
        viewNo=initial_view_no + 1,
        stableCheckpoint=4,
        prepared=[],
        preprepared=[],
        checkpoints=[]
    )
    frm = other_item(validators, name)
    service._network.process_incoming(vc, frm)

    assert len(service._network.sent_messages) == 1
    msg, dst = service._network.sent_messages[0]
    assert dst == service._data.primary_name
    assert isinstance(msg, ViewChangeAck)
    assert msg.viewNo == vc.viewNo
    assert msg.name == frm
    assert msg.digest == ViewChangeService._view_change_digest(vc)


def test_primary_doesnt_respond_to_view_change_message(
        some_item, validators, primary, view_change_service, initial_view_no):
    name = primary(initial_view_no + 1)
    service = view_change_service(name)

    vc = ViewChange(
        viewNo=initial_view_no + 1,
        stableCheckpoint=4,
        prepared=[],
        preprepared=[],
        checkpoints=[]
    )
    frm = some_item(validators, exclude=[primary])
    service._network.process_incoming(vc, frm)
    assert len(service._network.sent_messages) == 0


@pytest.mark.skip
def test_new_view_message_is_sent_once_when_view_change_certificate_is_reached(
        initial_view_no, consensus_data_of_primary_in_next_view, validators):
    service = ViewChangeService(consensus_data_of_primary_in_next_view, InternalBus(), MockNetwork())

    non_primaries = [name for name in validators if name != service._data.name]
    for vc_src in validators:
        for ack_src in non_primaries:
            if ack_src == vc_src:
                continue

            vc = ViewChangeAck(
                viewNo=initial_view_no + 1,
                name=vc_src,
                digest='digest_of_view_change_message'
            )
            service._network.process_incoming(vc, ack_src)

    assert len(service._network.sent_messages) == 1
    msg, dst = service._network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, NewView)
    assert msg.viewNo == initial_view_no + 1


def test_view_change_digest_is_256_bit_hexdigest(random):
    vc = view_change_message(random)
    digest = ViewChangeService._view_change_digest(vc)
    assert isinstance(digest, str)
    assert len(digest) == 64
    assert all(v in string.hexdigits for v in digest)


def test_different_view_change_messages_have_different_digests(random):
    vc = view_change_message(random)
    other_vc = view_change_message(random)
    assert ViewChangeService._view_change_digest(vc) != ViewChangeService._view_change_digest(other_vc)
