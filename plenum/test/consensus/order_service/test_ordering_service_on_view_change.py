import pytest

from plenum.common.messages.internal_messages import ViewChangeStarted, NewViewAccepted, NewViewCheckpointsApplied
from plenum.common.messages.node_messages import OldViewPrePrepareRequest, OldViewPrePrepareReply
from plenum.common.util import updateNamedTuple
from plenum.server.consensus.batch_id import BatchID
from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.consensus.utils import preprepare_to_batch_id
from plenum.server.replica_helper import generateName
from plenum.test.consensus.helper import copy_shared_data, create_batches, \
    check_service_changed_only_owned_fields_in_shared_data, create_new_view, \
    create_pre_prepares, create_batches_from_preprepares
from plenum.test.consensus.order_service.helper import check_prepares_sent, check_request_old_view_preprepares_sent, \
    check_reply_old_view_preprepares_sent
from plenum.test.helper import create_pre_prepare_no_bls, generate_state_root, create_prepare, create_commit_no_bls_sig
from plenum.test.consensus.order_service.conftest import orderer as _orderer

applied_pre_prepares = 0


@pytest.fixture(params=[True, False], ids=['Primary', 'Non-Primary'])
def is_primary(request):
    return request.param == 'Primary'


@pytest.fixture()
def orderer(_orderer, is_primary, ):
    _orderer.name = 'Alpha:0'
    _orderer._data.primary_name = 'some_node:0' if not is_primary else orderer.name

    def _apply_and_validate_applied_pre_prepare_fake(pp, sender):
        global applied_pre_prepares
        applied_pre_prepares += 1

    _orderer._can_process_pre_prepare = lambda pp, sender: None
    _orderer._apply_and_validate_applied_pre_prepare = _apply_and_validate_applied_pre_prepare_fake

    return _orderer


@pytest.fixture()
def initial_view_no():
    return 3


@pytest.fixture()
def pre_prepares(initial_view_no):
    return create_pre_prepares(view_no=initial_view_no)


@pytest.fixture(params=['all', 'first', 'last', 'no'])
def stored_old_view_pre_prepares(request, pre_prepares):
    if request.param == 'all':
        return pre_prepares
    if request.param == 'first':
        return [pre_prepares[0]]
    if request.param == 'last':
        return [pre_prepares[-1]]
    return []


@pytest.fixture(params=['all', 'first', 'last', 'no'])
def requested_old_view_pre_prepares(request, pre_prepares):
    if request.param == 'all':
        return pre_prepares
    if request.param == 'first':
        return [pre_prepares[0]]
    if request.param == 'last':
        return [pre_prepares[-1]]
    return []


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
                                   view_no=0, pp_seq_no=10, inst_id=0,
                                   audit_txn_root="HSai3sMHKeAva4gWMabDrm1yNhezvPHfXnGyHf2ex1L4")
    prepare = create_prepare(req_key=(0, 10),
                             state_root=generate_state_root(), inst_id=0)
    commit = create_commit_no_bls_sig(req_key=(0, 10), inst_id=0)
    key = (pp.viewNo, pp.ppSeqNo)

    orderer.prePrepares[key] = pp
    orderer.prepares[key] = prepare
    orderer.commits[key] = commit
    orderer.pre_prepare_tss[key][pp.auditTxnRootHash, "Node1"] = 1234
    orderer.prePreparesPendingFinReqs.append(pp)
    orderer.prePreparesPendingPrevPP[key] = pp
    orderer.sent_preprepares[key] = pp
    orderer.batches[key] = [pp.ledgerId, pp.discarded,
                            pp.ppTime, generate_state_root(), len(pp.reqIdr)]
    orderer.ordered.add(*key)

    internal_bus.send(ViewChangeStarted(view_no=4))

    assert not orderer.prePrepares
    assert not orderer.prepares
    assert not orderer.commits

    assert not orderer.pre_prepare_tss
    assert not orderer.prePreparesPendingFinReqs
    assert not orderer.prePreparesPendingPrevPP
    assert not orderer.sent_preprepares
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
    orderer.sent_preprepares[(pp2.viewNo, pp2.ppSeqNo)] = pp2
    orderer.sent_preprepares[(pp4.viewNo, pp4.ppSeqNo)] = pp4
    assert not orderer.old_view_preprepares

    internal_bus.send(ViewChangeStarted(view_no=4))

    assert orderer.old_view_preprepares[(pp1.viewNo, pp1.ppSeqNo, pp1.digest)] == pp1
    assert orderer.old_view_preprepares[(pp2.viewNo, pp2.ppSeqNo, pp2.digest)] == pp2
    assert orderer.old_view_preprepares[(pp3.viewNo, pp3.ppSeqNo, pp3.digest)] == pp3
    assert orderer.old_view_preprepares[(pp4.viewNo, pp4.ppSeqNo, pp4.digest)] == pp4

    # next calls append to existing data
    orderer.prePrepares[(pp5.viewNo, pp5.ppSeqNo)] = pp5
    orderer.sent_preprepares[(pp6.viewNo, pp6.ppSeqNo)] = pp6

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


@pytest.mark.parametrize('all_ordered', [True, False], ids=['All-ordered', 'All-non-ordered'])
def test_process_preprepare_on_new_view_checkpoint_applied(internal_bus, external_bus,
                                                           orderer, is_primary,
                                                           all_ordered,
                                                           initial_view_no,
                                                           pre_prepares, stored_old_view_pre_prepares):
    # !!!SETUP!!!
    orderer._data.view_no = initial_view_no + 1
    batches = create_batches_from_preprepares(pre_prepares)
    orderer._data.prev_view_prepare_cert = batches[-1].pp_seq_no

    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200,
                               batches=batches)

    # emulate that we received all PrePrepares before View Change
    orderer._update_old_view_preprepares(stored_old_view_pre_prepares)

    # emulate that we've already ordered the PrePrepares
    if all_ordered and stored_old_view_pre_prepares:
        orderer.last_ordered_3pc = (initial_view_no, stored_old_view_pre_prepares[-1].ppSeqNo)

    # !!!EXECUTE!!!
    # send NewViewCheckpointsApplied
    internal_bus.send(NewViewCheckpointsApplied(view_no=initial_view_no + 1,
                                                view_changes=new_view.viewChanges,
                                                checkpoint=new_view.checkpoint,
                                                batches=new_view.batches))

    # !!!CHECK!!!
    if not orderer.is_master:
        # no re-ordering is expected on non-master
        assert orderer._data.preprepared == []
        assert orderer._data.prepared == []
        return

    # check that PPs were added
    stored_batch_ids = [preprepare_to_batch_id(pp) for pp in stored_old_view_pre_prepares]
    assert orderer._data.preprepared == [BatchID(view_no=initial_view_no + 1, pp_view_no=initial_view_no,
                                                 pp_seq_no=batch_id.pp_seq_no, pp_digest=batch_id.pp_digest)
                                         for batch_id in new_view.batches if batch_id in stored_batch_ids]

    # check that sentPrePrepares is updated in case of Primary and prePrepares in case of non-primary
    updated_prepares_collection = orderer.prePrepares if not is_primary else orderer.sent_preprepares
    non_updated_prepares_collection = orderer.sent_preprepares if not is_primary else orderer.prePrepares
    for pp in stored_old_view_pre_prepares:
        new_pp = updateNamedTuple(pp, viewNo=initial_view_no + 1, originalViewNo=pp.viewNo)
        assert (initial_view_no + 1, new_pp.ppSeqNo) in updated_prepares_collection
        assert updated_prepares_collection[(initial_view_no + 1, new_pp.ppSeqNo)] == new_pp
    assert not non_updated_prepares_collection

    # check that Prepare is sent in case of non primary
    if not is_primary:
        check_prepares_sent(external_bus, stored_old_view_pre_prepares, initial_view_no + 1)
    else:
        #  only MessageReqs are sent
        assert len(external_bus.sent_messages) == len(pre_prepares) - len(stored_old_view_pre_prepares)

    # we don't have a quorum of Prepares yet
    assert orderer._data.prepared == []

    # check that missing PrePrepares have been requested
    expected_requested_batches = [batch_id for batch_id in new_view.batches if batch_id not in stored_batch_ids]
    check_request_old_view_preprepares_sent(external_bus, expected_requested_batches)


def test_send_reply_on_old_view_pre_prepares_request(external_bus, orderer,
                                                     initial_view_no,
                                                     stored_old_view_pre_prepares,
                                                     requested_old_view_pre_prepares):
    # Setup
    orderer._data.view_no = initial_view_no + 2

    orderer._update_old_view_preprepares(stored_old_view_pre_prepares)

    # Receive OldViewPrePrepareRequest req
    batches = [preprepare_to_batch_id(pp) for pp in requested_old_view_pre_prepares]
    req = OldViewPrePrepareRequest(0, batches)
    frm = "node1"
    orderer._network.process_incoming(req, generateName(frm, orderer._data.inst_id))

    # Check that OldViewPrePrepareReply is sent  for all requested PrePrepares
    if not orderer.is_master:
        assert len(external_bus.sent_messages) == 0
        return
    # equal to set's union operation
    expected_pps = [i for i in stored_old_view_pre_prepares if i in requested_old_view_pre_prepares]
    expected_pps = sorted(expected_pps, key=lambda pp: pp.ppSeqNo)
    check_reply_old_view_preprepares_sent(external_bus, frm, expected_pps)


def test_process_preprepare_on_old_view_pre_prepares_reply(external_bus, internal_bus,
                                                           orderer, is_primary,
                                                           initial_view_no,
                                                           pre_prepares):
    # !!!SETUP!!!
    orderer._data.view_no = initial_view_no + 1
    new_view = create_new_view(initial_view_no=initial_view_no, stable_cp=200,
                               batches=create_batches_from_preprepares(pre_prepares))
    orderer._data.new_view_votes.add_new_view(new_view, orderer._data.primary_name)
    orderer._data.prev_view_prepare_cert = new_view.batches[-1].pp_seq_no

    # !!!EXECUTE!!!
    rep = OldViewPrePrepareReply(0, [pp._asdict() for pp in pre_prepares])
    orderer._network.process_incoming(rep, generateName("node1", orderer._data.inst_id))

    # !!!CHECK!!!
    if not orderer.is_master:
        # no re-ordering is expected on non-master
        assert orderer._data.preprepared == []
        assert orderer._data.prepared == []
        return

    # check that PPs were added
    assert orderer._data.preprepared == [BatchID(view_no=initial_view_no + 1, pp_view_no=pp.viewNo,
                                                 pp_seq_no=pp.ppSeqNo, pp_digest=pp.digest)
                                         for pp in pre_prepares]

    # check that sent_preprepares is updated in case of Primary and prePrepares in case of non-primary
    updated_prepares_collection = orderer.prePrepares if not is_primary else orderer.sent_preprepares
    non_updated_prepares_collection = orderer.sent_preprepares if not is_primary else orderer.prePrepares
    for pp in pre_prepares:
        new_pp = updateNamedTuple(pp, viewNo=initial_view_no + 1, originalViewNo=pp.viewNo)
        assert (initial_view_no + 1, new_pp.ppSeqNo) in updated_prepares_collection
        assert updated_prepares_collection[(initial_view_no + 1, new_pp.ppSeqNo)] == new_pp
    assert not non_updated_prepares_collection

    # check that Prepare is sent in case of non primary
    if not is_primary:
        check_prepares_sent(external_bus, pre_prepares, initial_view_no + 1)
    else:
        assert len(external_bus.sent_messages) == 0

    # we don't have a quorum of Prepares yet
    assert orderer._data.prepared == []
