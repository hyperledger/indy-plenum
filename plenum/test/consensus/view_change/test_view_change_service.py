import random

import pytest
from unittest.mock import Mock

from plenum.common.messages.internal_messages import NeedViewChange, NewViewAccepted, ViewChangeStarted, \
    NewViewCheckpointsApplied
from plenum.server.consensus.view_change_storages import view_change_digest
from plenum.common.messages.node_messages import ViewChange, ViewChangeAck, NewView, Checkpoint, InstanceChange
from plenum.server.consensus.view_change_service import ViewChangeService
from plenum.server.replica_helper import generateName, getNodeName
from plenum.server.suspicion_codes import Suspicions
from plenum.test.checkpoints.helper import cp_digest
from plenum.test.consensus.helper import copy_shared_data, check_service_changed_only_owned_fields_in_shared_data, \
    create_new_view, create_view_change, create_new_view_from_vc, create_view_change_acks, create_batches


@pytest.fixture
def view_change_service_builder(consensus_data, timer, internal_bus, external_bus, stasher):
    def _service(name):
        data = consensus_data(name)
        service = ViewChangeService(data, timer, internal_bus, external_bus, stasher)
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


def test_updates_shared_data_on_need_view_change(internal_bus, view_change_service, initial_view_no, is_master):
    old_primary = view_change_service._data.primary_name
    old_primaries = view_change_service._data.primaries
    old_data = copy_shared_data(view_change_service._data)
    internal_bus.send(NeedViewChange())

    assert view_change_service._data.view_no == initial_view_no + 1
    assert view_change_service._data.waiting_for_new_view == is_master
    assert view_change_service._data.primary_name != old_primary
    assert view_change_service._data.primaries != old_primaries
    new_data = copy_shared_data(view_change_service._data)
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)

    old_primary = view_change_service._data.primary_name
    old_primaries = view_change_service._data.primaries
    old_data = copy_shared_data(view_change_service._data)
    internal_bus.send(NeedViewChange(view_no=initial_view_no + 3))

    assert view_change_service._data.view_no == initial_view_no + 3
    assert view_change_service._data.waiting_for_new_view == is_master
    assert view_change_service._data.primary_name != old_primary
    assert view_change_service._data.primaries != old_primaries
    new_data = copy_shared_data(view_change_service._data)
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)


def test_do_nothing_on_view_change_started(internal_bus, view_change_service):
    view_change_service._data.waiting_for_new_view = False
    view_change_service._data.view_no = 1
    view_change_service._data.primary_name = "Alpha"
    view_change_service._data.primaries = ["Alpha", "Beta"]
    old_data = copy_shared_data(view_change_service._data)

    internal_bus.send(ViewChangeStarted(view_no=4))

    new_data = copy_shared_data(view_change_service._data)
    assert old_data == new_data


def test_update_shared_data_on_new_view_accepted(internal_bus, view_change_service, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    view_change_service._data.waiting_for_new_view = False
    view_change_service._data.view_no = 1
    view_change_service._data.primary_name = "Alpha"
    view_change_service._data.primaries = ["Alpha", "Beta"]
    view_change_service._data.prev_view_prepare_cert = 1
    old_data = copy_shared_data(view_change_service._data)

    new_view = create_new_view(initial_view_no=3, stable_cp=200)
    internal_bus.send(NewViewAccepted(view_no=4,
                                      view_changes=new_view.viewChanges,
                                      checkpoint=new_view.checkpoint,
                                      batches=new_view.batches))

    new_data = copy_shared_data(view_change_service._data)
    # For now prev_view_prepare_cert is set on finish_view_change stage
    assert view_change_service._data.prev_view_prepare_cert == 1
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)


def test_setup_prev_view_prepare_cert_on_vc_finished(internal_bus, view_change_service, is_master):
    if not is_master:
        return

    view_change_service._data.waiting_for_new_view = True
    view_change_service._data.prev_view_prepare_cert = 1
    new_view = create_new_view(initial_view_no=3, stable_cp=200)
    view_change_service._new_view = new_view
    view_change_service._finish_view_change()
    assert view_change_service._data.prev_view_prepare_cert == new_view.batches[-1].pp_seq_no
    assert not view_change_service._data.waiting_for_new_view


def test_update_shared_data_on_new_view_accepted_no_batches(internal_bus, view_change_service, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    view_change_service._data.waiting_for_new_view = False
    view_change_service._data.view_no = 1
    view_change_service._data.primary_name = "Alpha"
    view_change_service._data.primaries = ["Alpha", "Beta"]
    view_change_service._data.prev_view_prepare_cert = 1
    old_data = copy_shared_data(view_change_service._data)

    new_view = create_new_view(initial_view_no=3, stable_cp=200, batches=[])
    internal_bus.send(NewViewAccepted(view_no=4,
                                      view_changes=new_view.viewChanges,
                                      checkpoint=new_view.checkpoint,
                                      batches=new_view.batches))

    new_data = copy_shared_data(view_change_service._data)
    # For now prev_view_prepare_cert is set on finish_view_change stage
    assert view_change_service._data.prev_view_prepare_cert == 1
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)


def test_do_nothing_on_new_view_checkpoint_applied(internal_bus, view_change_service):
    view_change_service._data.waiting_for_new_view = False
    view_change_service._data.view_no = 1
    view_change_service._data.primary_name = "Alpha"
    view_change_service._data.primaries = ["Alpha", "Beta"]
    old_data = copy_shared_data(view_change_service._data)

    new_view = create_new_view(initial_view_no=3, stable_cp=200)
    internal_bus.send(NewViewCheckpointsApplied(view_no=4,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    new_data = copy_shared_data(view_change_service._data)
    assert old_data == new_data


def test_start_view_change_sends_view_change_started(internal_bus, view_change_service, initial_view_no, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    handler = Mock()
    internal_bus.subscribe(ViewChangeStarted, handler)

    internal_bus.send(NeedViewChange())
    handler.assert_called_once_with(ViewChangeStarted(view_no=initial_view_no + 1))

    internal_bus.send(NeedViewChange(view_no=5))
    handler.assert_called_with(ViewChangeStarted(view_no=5))


def test_start_view_change_broadcasts_view_change_message(internal_bus, external_bus, view_change_service,
                                                          initial_view_no, is_master):
    internal_bus.send(NeedViewChange())

    if is_master:
        assert len(external_bus.sent_messages) == 1
        msg, dst = external_bus.sent_messages[0]
        assert dst is None  # message was broadcast
        assert isinstance(msg, ViewChange)
        assert msg.viewNo == initial_view_no + 1
        assert msg.stableCheckpoint == view_change_service._data.stable_checkpoint
    else:
        assert len(external_bus.sent_messages) == 0


def test_non_primary_responds_to_view_change_message_with_view_change_ack_to_new_primary(
        internal_bus, external_bus, some_item, other_item, validators, primary, view_change_service_builder,
        initial_view_no, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    next_view_no = initial_view_no + 1
    non_primary_name = some_item(validators, exclude=[primary(next_view_no)])
    service = view_change_service_builder(non_primary_name)

    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    vc = create_view_change(initial_view_no)
    frm = other_item(validators, exclude=[non_primary_name])
    external_bus.process_incoming(vc, generateName(frm, service._data.inst_id))

    assert len(external_bus.sent_messages) == 1
    msg, dst = external_bus.sent_messages[0]
    assert dst == [getNodeName(service._data.primary_name)]
    assert isinstance(msg, ViewChangeAck)
    assert msg.viewNo == vc.viewNo
    assert msg.name == frm
    assert msg.digest == view_change_digest(vc)


def test_primary_doesnt_respond_to_view_change_message(
        some_item, validators, primary, external_bus, view_change_service_builder, initial_view_no,
        view_change_message, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    name = primary(initial_view_no + 1)
    service = view_change_service_builder(name)

    vc = create_view_change(initial_view_no)
    frm = some_item(validators, exclude=[name])
    external_bus.process_incoming(vc, generateName(frm, service._data.inst_id))

    assert len(external_bus.sent_messages) == 0


def test_new_view_message_is_sent_by_primary_when_view_change_certificate_is_reached(
        internal_bus, external_bus, validators, primary, view_change_service_builder, initial_view_no,
        view_change_acks, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    primary_name = primary(initial_view_no + 1)
    service = view_change_service_builder(primary_name)

    # start view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    vc = create_view_change(initial_view_no)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in view_change_acks(vc, vc_frm, primary_name, len(validators) - 2):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

    # check that NewView has been sent
    assert len(external_bus.sent_messages) == 1
    msg, dst = external_bus.sent_messages[0]
    assert dst is None  # message was broadcast
    assert isinstance(msg, NewView)
    assert msg.viewNo == initial_view_no + 1


def test_new_view_message_is_not_sent_by_non_primary_when_view_change_certificate_is_reached(
        internal_bus, external_bus, validators, primary, view_change_service_builder, initial_view_no, some_item,
        is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    next_view_no = initial_view_no + 1
    primary_name = primary(next_view_no)
    non_primary_name = some_item(validators, exclude=[primary_name])
    service = view_change_service_builder(non_primary_name)

    # start view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    vc = create_view_change(initial_view_no)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in create_view_change_acks(vc, vc_frm, non_primaries):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

    # check that NewView hasn't been sent
    assert all(not isinstance(msg, NewView) for msg in external_bus.sent_messages)


def test_view_change_finished_is_sent_by_primary_once_view_change_certificate_is_reached(internal_bus, external_bus,
                                                                                         validators,
                                                                                         primary,
                                                                                         view_change_service_builder,
                                                                                         initial_view_no, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    handler = Mock()
    internal_bus.subscribe(NewViewAccepted, handler)

    primary_name = primary(initial_view_no + 1)
    service = view_change_service_builder(primary_name)

    # start view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()
    old_data = copy_shared_data(service._data)

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    non_primaries = random.sample(non_primaries, service._data.quorums.view_change.value)
    vc = create_view_change(initial_view_no)
    new_view = create_new_view_from_vc(vc, non_primaries)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in create_view_change_acks(vc, vc_frm, non_primaries):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

    # check that NewViewAccepted has been sent
    expected_finish_vc = NewViewAccepted(view_no=initial_view_no + 1,
                                         view_changes=new_view.viewChanges,
                                         checkpoint=new_view.checkpoint,
                                         batches=new_view.batches)
    handler.assert_called_with(expected_finish_vc)

    # check that shared data is updated
    new_data = copy_shared_data(service._data)
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)
    assert service._data.view_no == initial_view_no + 1
    assert not service._data.waiting_for_new_view


def test_view_change_finished_is_sent_by_non_primary_once_view_change_certificate_is_reached_and_new_view_from_primary(
        internal_bus, external_bus, validators, primary, view_change_service_builder, initial_view_no, some_item,
        is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    handler = Mock()
    internal_bus.subscribe(NewViewAccepted, handler)

    next_view_no = initial_view_no + 1
    primary_name = primary(next_view_no)
    non_primary_name = some_item(validators, exclude=[primary_name])
    service = view_change_service_builder(non_primary_name)
    vc = create_view_change(initial_view_no)
    service._data.preprepared = vc.preprepared
    service._data.prepared = vc.prepared
    service._data.stable_checkpoint = vc.stableCheckpoint
    service._data.checkpoints = vc.checkpoints
    old_data = copy_shared_data(service._data)

    # start view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    non_primaries = random.sample(non_primaries, service._data.quorums.view_change.value)
    new_view = create_new_view_from_vc(vc, non_primaries)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in create_view_change_acks(vc, vc_frm, non_primaries):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

        # check that NewViewAccepted hasn't been sent if NewView is from non-primary
        external_bus.process_incoming(new_view, generateName(non_primary_name, service._data.inst_id))
    handler.assert_not_called()
    assert service._data.view_no == initial_view_no + 1
    assert service._data.waiting_for_new_view

    # check that NewViewAccepted has been sent if NewView is from primary
    external_bus.process_incoming(new_view, generateName(primary_name, service._data.inst_id))
    expected_finish_vc = NewViewAccepted(view_no=initial_view_no + 1,
                                         view_changes=new_view.viewChanges,
                                         checkpoint=new_view.checkpoint,
                                         batches=new_view.batches)
    handler.assert_called_with(expected_finish_vc)

    # check that shared data is updated
    new_data = copy_shared_data(service._data)
    check_service_changed_only_owned_fields_in_shared_data(ViewChangeService, old_data, new_data)
    assert service._data.view_no == initial_view_no + 1
    assert not service._data.waiting_for_new_view


def test_send_instance_change_on_new_view_with_incorrect_checkpoint(internal_bus, external_bus, validators, primary,
                                                                    view_change_service_builder,
                                                                    initial_view_no,
                                                                    some_item, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    next_view_no = initial_view_no + 1
    primary_name = primary(next_view_no)
    non_primary_name = some_item(validators, exclude=[primary_name])
    service = view_change_service_builder(non_primary_name)

    vc = create_view_change(initial_view_no)
    service._data.preprepared = vc.preprepared
    service._data.prepared = vc.prepared
    service._data.stable_checkpoint = vc.stableCheckpoint
    service._data.checkpoints = vc.checkpoints

    # start view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    non_primaries = random.sample(non_primaries, service._data.quorums.view_change.value)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in create_view_change_acks(vc, vc_frm, non_primaries):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

    cp = Checkpoint(instId=0, viewNo=initial_view_no, seqNoStart=0, seqNoEnd=1000, digest=cp_digest(1000))
    new_view = create_new_view_from_vc(vc, non_primaries, checkpoint=cp)

    # send NewView by Primary
    init_network_msg_count = len(external_bus.sent_messages)
    external_bus.process_incoming(new_view, generateName(primary_name, service._data.inst_id))

    # we don't go to new view, just send Instance Change
    assert service._data.view_no == initial_view_no + 1
    assert init_network_msg_count + 1 == len(external_bus.sent_messages)
    msg, dst = external_bus.sent_messages[-1]
    assert dst is None  # broadcast
    assert isinstance(msg, InstanceChange)
    assert msg.viewNo == initial_view_no + 2
    assert msg.reason == Suspicions.NEW_VIEW_INVALID_CHECKPOINTS.code


def test_send_instance_change_on_new_view_with_incorrect_batches(internal_bus, external_bus, validators, primary,
                                                                 view_change_service_builder,
                                                                 initial_view_no,
                                                                 some_item, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    next_view_no = initial_view_no + 1
    primary_name = primary(next_view_no)
    non_primary_name = some_item(validators, exclude=[primary_name])
    service = view_change_service_builder(non_primary_name)

    vc = create_view_change(initial_view_no)
    service._data.preprepared = vc.preprepared
    service._data.prepared = vc.prepared
    service._data.stable_checkpoint = vc.stableCheckpoint
    service._data.checkpoints = vc.checkpoints

    # start view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    non_primaries = random.sample(non_primaries, service._data.quorums.view_change.value)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in create_view_change_acks(vc, vc_frm, non_primaries):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

    new_view = create_new_view_from_vc(vc, non_primaries, batches=create_batches(view_no=initial_view_no + 2))

    # send NewView by Primary
    init_network_msg_count = len(external_bus.sent_messages)
    external_bus.process_incoming(new_view, generateName(primary_name, service._data.inst_id))

    # we don't go to new view, just send Instance Change
    assert service._data.view_no == initial_view_no + 1
    assert init_network_msg_count + 1 == len(external_bus.sent_messages)
    msg, dst = external_bus.sent_messages[-1]
    assert dst is None  # broadcast
    assert isinstance(msg, InstanceChange)
    assert msg.viewNo == initial_view_no + 2
    assert msg.reason == Suspicions.NEW_VIEW_INVALID_BATCHES.code


def test_send_instance_change_on_timeout_no_new_view_received(internal_bus, external_bus,
                                                              view_change_service, timer,
                                                              initial_view_no, is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    internal_bus.send(NeedViewChange())

    init_network_msg_count = len(external_bus.sent_messages)
    timer.sleep(view_change_service._config.NEW_VIEW_TIMEOUT - 1)
    assert view_change_service._data.view_no == initial_view_no + 1
    assert init_network_msg_count == len(external_bus.sent_messages)

    timer.sleep(2)
    # we don't go to new view, just send Instance Change
    assert view_change_service._data.view_no == initial_view_no + 1
    assert init_network_msg_count + 1 == len(external_bus.sent_messages)
    msg, dst = external_bus.sent_messages[-1]
    assert dst is None  # broadcast
    assert isinstance(msg, InstanceChange)
    assert msg.viewNo == initial_view_no + 2
    assert msg.reason == Suspicions.INSTANCE_CHANGE_TIMEOUT.code

    timer.sleep(view_change_service._config.NEW_VIEW_TIMEOUT + 1)
    # we don't go to new view, just send Instance Change
    assert view_change_service._data.view_no == initial_view_no + 1
    assert init_network_msg_count + 2 == len(external_bus.sent_messages)
    msg, dst = external_bus.sent_messages[-1]
    assert dst is None  # broadcast
    assert isinstance(msg, InstanceChange)
    assert msg.viewNo == initial_view_no + 2
    assert msg.reason == Suspicions.INSTANCE_CHANGE_TIMEOUT.code


def test_send_instance_change_on_timeout_when_new_view_received_but_not_processed(internal_bus, external_bus, timer,
                                                                                  view_change_service, initial_view_no,
                                                                                  is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    internal_bus.send(NeedViewChange())
    init_network_msg_count = len(external_bus.sent_messages)
    new_view = create_new_view(initial_view_no=0, stable_cp=200)
    external_bus.process_incoming(new_view, view_change_service._data.primary_name)

    timer.sleep(view_change_service._config.NEW_VIEW_TIMEOUT + 1)

    # we don't go to new view, just send Instance Change
    assert view_change_service._data.view_no == initial_view_no + 1
    assert init_network_msg_count + 1 == len(external_bus.sent_messages)
    msg, dst = external_bus.sent_messages[-1]
    assert dst is None  # broadcast
    assert isinstance(msg, InstanceChange)
    assert msg.viewNo == initial_view_no + 2
    assert msg.reason == Suspicions.INSTANCE_CHANGE_TIMEOUT.code


def test_do_not_send_instance_change_on_timeout_when_view_change_finished_on_time(internal_bus, external_bus,
                                                                                  validators,
                                                                                  primary, view_change_service_builder,
                                                                                  timer,
                                                                                  initial_view_no,
                                                                                  is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    primary_name = primary(initial_view_no + 1)
    service = view_change_service_builder(primary_name)

    # start view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    vc = create_view_change(initial_view_no)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in create_view_change_acks(vc, vc_frm, non_primaries):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

    # check that view change is finished
    assert service._data.view_no == initial_view_no + 1
    assert not service._data.waiting_for_new_view
    assert len(external_bus.sent_messages) == 1
    msg, dst = external_bus.sent_messages[0]
    assert isinstance(msg, NewView)

    # make sure view change hasn't been started again
    timer.sleep(service._config.NEW_VIEW_TIMEOUT + 1)
    assert service._data.view_no == initial_view_no + 1
    assert len(external_bus.sent_messages) == 1
    msg, dst = external_bus.sent_messages[0]
    assert isinstance(msg, NewView)


def test_do_not_send_instance_change_on_timeout_when_multiple_view_change_finished_on_time(internal_bus, external_bus,
                                                                                           validators,
                                                                                           primary,
                                                                                           view_change_service_builder,
                                                                                           timer,
                                                                                           initial_view_no,
                                                                                           is_master):
    # TODO: Need to decide on how we handle this case
    if not is_master:
        return

    primary_name = primary(initial_view_no + 2)
    service = view_change_service_builder(primary_name)

    # start first view change
    internal_bus.send(NeedViewChange())

    # start second view change
    internal_bus.send(NeedViewChange())
    external_bus.sent_messages.clear()

    # receive quorum of ViewChanges and ViewChangeAcks
    non_primaries = [item for item in validators if item != primary_name]
    vc = create_view_change(initial_view_no + 1)
    for vc_frm in non_primaries:
        external_bus.process_incoming(vc, generateName(vc_frm, service._data.inst_id))
        for ack, ack_frm in create_view_change_acks(vc, vc_frm, non_primaries):
            external_bus.process_incoming(ack, generateName(ack_frm, service._data.inst_id))

    # check that view change is finished
    assert service._data.view_no == initial_view_no + 2
    assert not service._data.waiting_for_new_view
    assert len(external_bus.sent_messages) == 1
    msg, dst = external_bus.sent_messages[0]
    assert isinstance(msg, NewView)

    # make sure view change hasn't been started again
    timer.sleep(service._config.NEW_VIEW_TIMEOUT + 1)
    assert service._data.view_no == initial_view_no + 2
    assert len(external_bus.sent_messages) == 1
    msg, dst = external_bus.sent_messages[0]
    assert isinstance(msg, NewView)
