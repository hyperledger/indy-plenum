import pytest

from plenum.common.messages.internal_messages import ViewChangeStarted, NewViewAccepted, NewViewCheckpointsApplied
from plenum.common.request import Request
from plenum.common.util import updateNamedTuple
from plenum.server.consensus.consensus_shared_data import BatchID
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.server.propagator import ReqState
from plenum.test.consensus.helper import copy_shared_data, create_batches, \
    check_service_changed_only_owned_fields_in_shared_data, create_new_view, \
    create_pre_prepares, create_batches_from_preprepares
from plenum.test.consensus.order_service.helper import check_prepares_sent
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root, create_prepare, create_commit_no_bls_sig
from plenum.test.consensus.order_service.conftest import orderer as _orderer


@pytest.fixture()
def orderer(_orderer):
    _orderer._validator = OrderingServiceMsgValidator(_orderer._data)
    return _orderer


applied_pre_prepares = 0


@pytest.fixture()
def orderer_no_apply_pp(orderer):
    orderer._can_process_pre_prepare = lambda pp, sender: None

    def _apply_and_validate_applied_pre_prepare_fake(pp, sender):
        global applied_pre_prepares
        applied_pre_prepares += 1

    orderer._apply_and_validate_applied_pre_prepare = _apply_and_validate_applied_pre_prepare_fake
    return orderer


def test_update_shared_data_on_view_change_started(internal_bus, orderer):
    orderer._data.preprepared = create_batches(view_no=3)
    orderer._data.prepared = create_batches(view_no=3)
    old_data = copy_shared_data(orderer._data)

    internal_bus.send(ViewChangeStarted(view_no=4))

    new_data = copy_shared_data(orderer._data)
    check_service_changed_only_owned_fields_in_shared_data(OrderingService, old_data, new_data)
    assert orderer._data.preprepared == []
    assert orderer._data.prepared == []


def test_clear_data_on_view_change_started(internal_bus, orderer):
    pp = create_pre_prepare_no_bls(generate_state_root(),
                                   view_no=0, pp_seq_no=10, inst_id=0)
    prepare = create_prepare(req_key=(0, 10),
                             state_root=generate_state_root(), inst_id=0)
    commit = create_commit_no_bls_sig(req_key=(0, 10), inst_id=0)
    key = (pp.viewNo, pp.ppSeqNo)

    orderer.prePrepares[key] = pp
    orderer.prepares[key] = prepare
    orderer.commits[key] = commit
    orderer.requested_pre_prepares[key] = pp
    orderer.requested_prepares[key] = prepare
    orderer.requested_commits[key] = commit
    orderer.pre_prepare_tss[key][pp, "Node1"] = 1234
    orderer.prePreparesPendingFinReqs.append(pp)
    orderer.prePreparesPendingPrevPP[key] = pp
    orderer.sentPrePrepares[key] = pp
    orderer.batches[key] = [pp.ledgerId, pp.discarded,
                            pp.ppTime, generate_state_root(), len(pp.reqIdr)]
    orderer.ordered.add(*key)

    internal_bus.send(ViewChangeStarted(view_no=4))

    assert not orderer.prePrepares
    assert not orderer.prepares
    assert not orderer.commits

    assert not orderer.requested_pre_prepares
    assert not orderer.requested_prepares
    assert not orderer.requested_commits

    assert not orderer.pre_prepare_tss
    assert not orderer.prePreparesPendingFinReqs
    assert not orderer.prePreparesPendingPrevPP
    assert not orderer.sentPrePrepares
    assert not orderer.batches
    assert not orderer.ordered


def test_stores_old_pre_prepares_on_view_change_started(internal_bus, orderer):
    pp1 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=0, pp_seq_no=1, inst_id=0)
    pp2 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=0, pp_seq_no=2, inst_id=0)
    pp3 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=1, pp_seq_no=3, inst_id=0)
    pp4 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=2, pp_seq_no=4, inst_id=0)
    pp5 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=3, pp_seq_no=5, inst_id=0)
    pp6 = create_pre_prepare_no_bls(generate_state_root(),
                                    view_no=3, pp_seq_no=6, inst_id=0)

    orderer.prePrepares[(pp1.viewNo, pp1.ppSeqNo)] = pp1
    orderer.prePrepares[(pp3.viewNo, pp3.ppSeqNo)] = pp3
    orderer.sentPrePrepares[(pp2.viewNo, pp2.ppSeqNo)] = pp2
    orderer.sentPrePrepares[(pp4.viewNo, pp4.ppSeqNo)] = pp4
    assert not orderer.old_view_preprepares

    internal_bus.send(ViewChangeStarted(view_no=4))

    assert orderer.old_view_preprepares[(pp1.viewNo, pp1.ppSeqNo, pp1.digest)] == pp1
    assert orderer.old_view_preprepares[(pp2.viewNo, pp2.ppSeqNo, pp2.digest)] == pp2
    assert orderer.old_view_preprepares[(pp3.viewNo, pp3.ppSeqNo, pp3.digest)] == pp3
    assert orderer.old_view_preprepares[(pp4.viewNo, pp4.ppSeqNo, pp4.digest)] == pp4

    # next calls append to existing data
    orderer.prePrepares[(pp5.viewNo, pp5.ppSeqNo)] = pp5
    orderer.sentPrePrepares[(pp6.viewNo, pp6.ppSeqNo)] = pp6

    internal_bus.send(ViewChangeStarted(view_no=4))

    assert orderer.old_view_preprepares[(pp1.viewNo, pp1.ppSeqNo, pp1.digest)] == pp1
    assert orderer.old_view_preprepares[(pp2.viewNo, pp2.ppSeqNo, pp2.digest)] == pp2
    assert orderer.old_view_preprepares[(pp3.viewNo, pp3.ppSeqNo, pp3.digest)] == pp3
    assert orderer.old_view_preprepares[(pp4.viewNo, pp4.ppSeqNo, pp4.digest)] == pp4
    assert orderer.old_view_preprepares[(pp5.viewNo, pp5.ppSeqNo, pp5.digest)] == pp5
    assert orderer.old_view_preprepares[(pp6.viewNo, pp6.ppSeqNo, pp6.digest)] == pp6


def test_do_nothing_on_new_view_accepted(internal_bus, orderer):
    orderer._data.preprepared = create_batches(view_no=0)
    orderer._data.prepared = create_batches(view_no=0)
    old_data = copy_shared_data(orderer._data)

    initial_view_no = 3
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200)
    internal_bus.send(NewViewAccepted(view_no=initial_view_no + 1,
                                      view_changes=new_view.viewChanges,
                                      checkpoint=new_view.checkpoint,
                                      batches=new_view.batches))

    new_data = copy_shared_data(orderer._data)
    assert old_data == new_data


def test_update_shared_data_on_new_view_checkpoint_applied(internal_bus, orderer):
    initial_view_no = 3
    orderer._data.preprepared = []
    orderer._data.prepared = []
    orderer._data.view_no = initial_view_no + 1
    old_data = copy_shared_data(orderer._data)

    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200)
    internal_bus.send(NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    new_data = copy_shared_data(orderer._data)
    check_service_changed_only_owned_fields_in_shared_data(OrderingService, old_data, new_data)

    # Since we didn't order the PrePrepare from Batches, it should not be added into shared data
    # (we will request the PrePrepares instead, see next tests)
    assert orderer._data.preprepared == []
    assert orderer._data.prepared == []


def test_on_new_view_checkpoint_applied__non_primary__ordered__has_all_pre_prepares(internal_bus, external_bus,
                                                                                    orderer):
    initial_view_no = 3
    orderer._data.view_no = initial_view_no + 1
    orderer._data.primary_name = 'some_node:0'  # non-primary

    pre_prepares = create_pre_prepares(view_no=initial_view_no)
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200,
                               batches=create_batches_from_preprepares(pre_prepares))

    # emulate that we received all PrePrepares before View Change
    orderer._update_old_view_preprepares(pre_prepares)

    # emulate that we've already ordered the PrePrepares
    orderer.last_ordered_3pc = (initial_view_no + 1, pre_prepares[-1].ppSeqNo)

    # SEND NewViewCheckpointsApplied
    internal_bus.send(NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    if orderer.is_master:
        # check that PPs were added
        assert orderer._data.preprepared == [BatchID(view_no=initial_view_no + 1, pp_view_no=initial_view_no,
                                                     pp_seq_no=batch_id.pp_seq_no, pp_digest=batch_id.pp_digest)
                                             for batch_id in new_view.batches]
        for pp in pre_prepares:
            new_pp = updateNamedTuple(pp, viewNo=initial_view_no + 1, originalViewNo=pp.viewNo)
            assert (initial_view_no + 1, new_pp.ppSeqNo) in orderer.prePrepares
            assert orderer.prePrepares[(initial_view_no + 1, new_pp.ppSeqNo)] == new_pp
        assert not orderer.sentPrePrepares

        # check that Prepare is sent
        check_prepares_sent(external_bus, pre_prepares, initial_view_no + 1)

        # we don't have a quorum of Prepares yet
        assert orderer._data.prepared == []
    else:
        # no re-ordering is expected on non-master
        assert orderer._data.preprepared == []
        assert orderer._data.prepared == []


def test_on_new_view_checkpoint_applied__primary__ordered__has_all_pre_prepares(external_bus, internal_bus, orderer):
    initial_view_no = 3
    orderer._data.view_no = initial_view_no + 1
    orderer._data.primary_name = orderer.name  # primary

    pre_prepares = create_pre_prepares(view_no=initial_view_no)
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200,
                               batches=create_batches_from_preprepares(pre_prepares))

    # emulate that we received all PrePrepares before View Change
    orderer._update_old_view_preprepares(pre_prepares)

    # emulate that we've already ordered the PrePrepares
    orderer.last_ordered_3pc = (initial_view_no + 1, pre_prepares[-1].ppSeqNo)

    # SEND NewViewCheckpointsApplied
    internal_bus.send(NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    if orderer.is_master:
        # check that PPs were added
        assert orderer._data.preprepared == [BatchID(view_no=initial_view_no + 1, pp_view_no=initial_view_no,
                                                     pp_seq_no=batch_id.pp_seq_no, pp_digest=batch_id.pp_digest)
                                             for batch_id in new_view.batches]
        for pp in pre_prepares:
            new_pp = updateNamedTuple(pp, viewNo=initial_view_no + 1, originalViewNo=pp.viewNo)
            assert (initial_view_no + 1, new_pp.ppSeqNo) in orderer.sentPrePrepares
            assert orderer.sentPrePrepares[(initial_view_no + 1, new_pp.ppSeqNo)] == new_pp
        assert not orderer.prePrepares

        # check that no Prepares sent
        assert len(external_bus.sent_messages) == 0

        # we don't have a quorum of Prepares yet
        assert orderer._data.prepared == []
    else:
        # no re-ordering is expected on non-master
        assert orderer._data.preprepared == []
        assert orderer._data.prepared == []


def test_on_new_view_checkpoint_applied__non_primary__non_ordered__has_all_pre_prepares(internal_bus, external_bus,
                                                                                        orderer_no_apply_pp):
    orderer = orderer_no_apply_pp
    initial_view_no = 3
    orderer._data.view_no = initial_view_no + 1
    orderer._data.primary_name = 'some_node:0'  # non-primary
    applied_pre_prepares_before = applied_pre_prepares

    pre_prepares = create_pre_prepares(view_no=initial_view_no)
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200,
                               batches=create_batches_from_preprepares(pre_prepares))

    # emulate that we received all PrePrepares before View Change
    orderer._update_old_view_preprepares(pre_prepares)

    # SEND NewViewCheckpointsApplied
    internal_bus.send(NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    if orderer.is_master:
        # check that PPs were added
        assert orderer._data.preprepared == [BatchID(view_no=initial_view_no + 1, pp_view_no=initial_view_no,
                                                     pp_seq_no=batch_id.pp_seq_no, pp_digest=batch_id.pp_digest)
                                             for batch_id in new_view.batches]
        for pp in pre_prepares:
            new_pp = updateNamedTuple(pp, viewNo=initial_view_no + 1, originalViewNo=pp.viewNo)
            assert (initial_view_no + 1, new_pp.ppSeqNo) in orderer.prePrepares
            assert orderer.prePrepares[(initial_view_no + 1, new_pp.ppSeqNo)] == new_pp
        assert not orderer.sentPrePrepares

        # check that Prepare is sent
        check_prepares_sent(external_bus, pre_prepares, initial_view_no + 1)

        # we don't have a quorum of Prepares yet
        assert orderer._data.prepared == []

        # check that apply was called
        assert applied_pre_prepares - applied_pre_prepares_before == len(pre_prepares)
    else:
        # no re-ordering is expected on non-master
        assert orderer._data.preprepared == []
        assert orderer._data.prepared == []


def test_on_new_view_checkpoint_applied__primary__non_ordered__has_all_pre_prepares(internal_bus, external_bus,
                                                                                    orderer_no_apply_pp):
    orderer = orderer_no_apply_pp
    initial_view_no = 3
    orderer._data.view_no = initial_view_no + 1
    orderer._data.primary_name = orderer.name
    applied_pre_prepares_before = applied_pre_prepares

    pre_prepares = create_pre_prepares(view_no=initial_view_no)
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200,
                               batches=create_batches_from_preprepares(pre_prepares))

    # emulate that we received all PrePrepares before View Change
    orderer._update_old_view_preprepares(pre_prepares)

    # SEND NewViewCheckpointsApplied
    internal_bus.send(NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    if orderer.is_master:
        # check that PPs were added
        assert orderer._data.preprepared == [BatchID(view_no=initial_view_no + 1, pp_view_no=initial_view_no,
                                                     pp_seq_no=batch_id.pp_seq_no, pp_digest=batch_id.pp_digest)
                                             for batch_id in new_view.batches]
        for pp in pre_prepares:
            new_pp = updateNamedTuple(pp, viewNo=initial_view_no + 1, originalViewNo=pp.viewNo)
            assert (initial_view_no + 1, new_pp.ppSeqNo) in orderer.sentPrePrepares
            assert orderer.sentPrePrepares[(initial_view_no + 1, new_pp.ppSeqNo)] == new_pp
        assert not orderer.prePrepares

        # check that no Prepares sent
        assert len(external_bus.sent_messages) == 0

        # we don't have a quorum of Prepares yet
        assert orderer._data.prepared == []

        # check that apply was called
        assert applied_pre_prepares - applied_pre_prepares_before == len(pre_prepares)
    else:
        # no re-ordering is expected on non-master
        assert orderer._data.preprepared == []
        assert orderer._data.prepared == []
