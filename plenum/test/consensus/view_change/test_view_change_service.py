import pytest
from mock import Mock

from plenum.common.event_bus import InternalBus
from plenum.common.messages.internal_messages import NeedViewChange, ViewChangeFinished, ViewChangeStarted, ApplyNewView
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView
from plenum.server.consensus.consensus_shared_data import BatchID
from plenum.server.consensus.view_change_service import ViewChangeService, view_change_digest
from plenum.test.consensus.helper import copy_shared_data, check_service_changed_only_owned_fields_in_shared_data, \
    create_new_view

from plenum.test.helper import MockNetwork


@pytest.fixture
def internal_bus():
    return InternalBus()


@pytest.fixture
def view_change_service_builder(consensus_data, mock_timer, internal_bus):
    def _service(name):
        data = consensus_data(name)
        service = ViewChangeService(data, mock_timer, internal_bus, MockNetwork())
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


@pytest.fixture
def view_change_service(view_change_service_builder, validators, some_item):
    return view_change_service_builder(some_item(validators))


def test_updates_shared_data_on_need_view_change(internal_bus, view_change_service, initial_view_no):
    old_primary = view_change_service._data.primary_name
    old_primaries = view_change_service._data.primaries
    old_data = copy_shared_data(view_change_service._data)
    internal_bus.send(NeedViewChange())

    assert view_change_service._data.view_no == initial_view_no + 1
    assert view_change_service._data.waiting_for_new_view
    assert view_change_service._data.primary_name != old_primary
    assert view_change_service._data.primaries != old_primaries
    new_data = copy_shared_data(view_change_service._data)
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)

    old_primary = view_change_service._data.primary_name
    old_primaries = view_change_service._data.primaries
    old_data = copy_shared_data(view_change_service._data)
    internal_bus.send(NeedViewChange(view_no=initial_view_no + 3))

    assert view_change_service._data.view_no == initial_view_no + 3
    assert view_change_service._data.waiting_for_new_view
    assert view_change_service._data.primary_name != old_primary
    assert view_change_service._data.primaries != old_primaries
    new_data = copy_shared_data(view_change_service._data)
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)


def test_updates_shared_data_on_new_view(internal_bus, view_change_service, initial_view_no):
    internal_bus.send(NeedViewChange())
    view_change_service._new_view = NewView(initial_view_no + 1,
                                            [], 300,
                                            [BatchID(initial_view_no, 10, "d1"),
                                             BatchID(initial_view_no, 11, "d2"),
                                             BatchID(initial_view_no, 12, "d3")])
    view_change_service._finish_view_change()

    assert view_change_service._data.view_no == initial_view_no + 1
    assert not view_change_service._data.waiting_for_new_view


def test_do_nothing_on_view_change_started(internal_bus, view_change_service):
    view_change_service._data.waiting_for_new_view = False
    view_change_service._data.view_no = 1
    view_change_service._data.primary_name = "Alpha"
    view_change_service._data.primaries = ["Alpha", "Beta"]
    old_data = copy_shared_data(view_change_service._data)

    internal_bus.send(ViewChangeStarted(view_no=4))

    new_data = copy_shared_data(view_change_service._data)
    assert old_data == new_data


def test_do_nothing_on_view_change_finished(internal_bus, view_change_service):
    view_change_service._data.waiting_for_new_view = False
    view_change_service._data.view_no = 1
    view_change_service._data.primary_name = "Alpha"
    view_change_service._data.primaries = ["Alpha", "Beta"]
    old_data = copy_shared_data(view_change_service._data)

    new_view = create_new_view(initial_view_no=3, stable_cp=200)
    internal_bus.send(ViewChangeFinished(view_no=4,
                                         view_changes=new_view.viewChanges,
                                         checkpoint=new_view.checkpoint,
                                         batches=new_view.batches))

    new_data = copy_shared_data(view_change_service._data)
    assert old_data == new_data


def test_do_nothing_on_apply_new_view(internal_bus, view_change_service):
    view_change_service._data.waiting_for_new_view = False
    view_change_service._data.view_no = 1
    view_change_service._data.primary_name = "Alpha"
    view_change_service._data.primaries = ["Alpha", "Beta"]
    old_data = copy_shared_data(view_change_service._data)

    new_view = create_new_view(initial_view_no=3, stable_cp=200)
    internal_bus.send(ApplyNewView(view_no=4,
                                   view_changes=new_view.viewChanges,
                                   checkpoint=new_view.checkpoint,
                                   batches=new_view.batches))

    new_data = copy_shared_data(view_change_service._data)
    assert old_data == new_data


def test_start_view_change_sends_view_change_started(internal_bus, view_change_service, initial_view_no):
    handler = Mock()
    internal_bus.subscribe(ViewChangeStarted, handler)

    internal_bus.send(NeedViewChange())
    handler.assert_called_once_with(ViewChangeStarted(view_no=initial_view_no + 1))

    internal_bus.send(NeedViewChange(view_no=5))
    handler.assert_called_with(ViewChangeStarted(view_no=5))


def test_finish_view_change_sends_view_change_finished(internal_bus, view_change_service, initial_view_no):
    handler = Mock()
    internal_bus.subscribe(ViewChangeFinished, handler)

    new_view = create_new_view(initial_view_no, 200)

    internal_bus.send(NeedViewChange())
    view_change_service._new_view = new_view
    expected_finish_vc = ViewChangeFinished(view_no=initial_view_no + 1,
                                            view_changes=new_view.viewChanges,
                                            checkpoint=new_view.checkpoint,
                                            batches=new_view.batches)
    view_change_service._finish_view_change()

    handler.assert_called_once_with(expected_finish_vc)


def test_start_view_change_broadcasts_view_change_message(internal_bus, view_change_service,
                                                          initial_view_no):
    internal_bus.send(NeedViewChange())

    assert len(view_change_service._network.sent_messages) == 1
    msg, dst = view_change_service._network.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, ViewChange)
    assert msg.viewNo == initial_view_no + 1
    assert msg.stableCheckpoint == view_change_service._data.stable_checkpoint


def test_non_primary_responds_to_view_change_message_with_view_change_ack_to_new_primary(
        internal_bus, some_item, other_item, validators, primary, view_change_service_builder, initial_view_no,
        view_change_message):
    next_view_no = initial_view_no + 1
    non_primary_name = some_item(validators, exclude=[primary(next_view_no)])
    service = view_change_service_builder(non_primary_name)

    internal_bus.send(NeedViewChange())
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
        some_item, validators, primary, view_change_service_builder, initial_view_no, view_change_message):
    name = primary(initial_view_no + 1)
    service = view_change_service_builder(name)

    vc = view_change_message(initial_view_no + 1)
    frm = some_item(validators, exclude=[name])
    service._network.process_incoming(vc, frm)

    assert len(service._network.sent_messages) == 0


def test_new_view_message_is_sent_once_when_view_change_certificate_is_reached(
        internal_bus, validators, primary, view_change_service_builder, initial_view_no, view_change_message,
        view_change_acks):
    primary_name = primary(initial_view_no + 1)
    service = view_change_service_builder(primary_name)

    internal_bus.send(NeedViewChange())
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
