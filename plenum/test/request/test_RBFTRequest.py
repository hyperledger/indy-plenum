import pytest
from copy import deepcopy

from plenum.common.request import Request
from plenum.server.quorums import Quorum
from plenum.server.tpcrequest import TPCRequest
from plenum.server.rbftrequest import RBFTReqState
from plenum.test.request.helper import check_transitions, check_statefuls

# TODO tests for:
#   - dry for all events
#   - separate tests for each get-API (e.g. is_... methods)

# FIXTURES
@pytest.fixture
def rbft_propagation(rbft_request):
    return deepcopy(rbft_request)

@pytest.fixture
def rbft_finalized(rbft_propagation):
    rbftr = deepcopy(rbft_propagation)
    rbftr.on_propagate(rbftr.request, 'me', Quorum(2))
    rbftr.on_propagate(rbftr.request, 'not_me', Quorum(2))
    return rbftr

@pytest.fixture
def rbft_forwarded(rbft_finalized, master_inst_id, backup_inst_id):
    rbftr = deepcopy(rbft_finalized)
    rbftr.on_forward((master_inst_id, backup_inst_id))
    return rbftr

@pytest.fixture
def rbft_master_rejected(rbft_forwarded, master_inst_id):
    rbftr = deepcopy(rbft_forwarded)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Reject((0, 1)))
    return rbftr

@pytest.fixture
def rbft_rejected(rbft_master_rejected):
    return deepcopy(rbft_master_rejected)

@pytest.fixture
def rbft_master_ordered_applied(rbft_forwarded, master_inst_id):
    rbftr = deepcopy(rbft_forwarded)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Accept((0, 1)))
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Order())
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Apply())
    return rbftr

@pytest.fixture
def rbft_committed(rbft_master_ordered_applied, master_inst_id):
    rbftr = deepcopy(rbft_master_ordered_applied)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Commit())
    return rbftr

@pytest.fixture
def rbft_replyed(rbft_committed):
    rbftr = deepcopy(rbft_committed)
    rbftr.on_reply()
    return rbftr

@pytest.fixture
def rbft_executed_not_cleaned(rbft_replyed):
    rbftr = deepcopy(rbft_replyed)
    rbftr.on_execute()
    return rbftr

@pytest.fixture
def rbft_executed_not_some_cleaned(rbft_executed_not_cleaned, master_inst_id):
    rbftr = deepcopy(rbft_executed_not_cleaned)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Clean())
    return rbftr

@pytest.fixture
def rbft_executed_and_cleaned(rbft_executed_not_some_cleaned, backup_inst_id):
    rbftr = deepcopy(rbft_executed_not_some_cleaned)
    rbftr.on_tpcevent(backup_inst_id, TPCRequest.Clean())
    return rbftr

@pytest.fixture
def rbft_detached(rbft_executed_and_cleaned):
    return deepcopy(rbft_executed_and_cleaned)

@pytest.fixture
def rbftr_all(
        rbft_propagation,
        rbft_finalized,
        rbft_forwarded,
        rbft_master_rejected,
        rbft_master_ordered_applied,
        rbft_committed,
        rbft_replyed,
        rbft_executed_not_cleaned,
        rbft_executed_not_some_cleaned,
        rbft_executed_and_cleaned,
        rbft_detached):
    return (rbft_propagation,
            rbft_finalized,
            rbft_forwarded,
            rbft_master_rejected,
            rbft_master_ordered_applied,
            rbft_committed,
            rbft_replyed,
            rbft_executed_not_cleaned,
            rbft_executed_not_some_cleaned,
            rbft_executed_and_cleaned,
            rbft_detached)

# TESTS
def test_initial_state(rbft_request):
    assert rbft_request.state() == RBFTReqState.Propagation

def test_tr_from_propagation(rbft_propagation):
    check_transitions(
        rbft_propagation,
        RBFTReqState,
        (RBFTReqState.Finalized,)
    )

def test_tr_from_finalized(rbft_finalized):
    check_transitions(
        rbft_finalized,
        RBFTReqState,
        (RBFTReqState.Forwarded,)
    )

def test_tr_from_forwarded(rbft_forwarded):
    check_transitions(
        rbft_forwarded,
        RBFTReqState,
        tuple()
    )

def test_master_rejected(rbft_master_rejected):
    assert rbft_master_rejected.state() == RBFTReqState.Rejected

def test_tr_from_rejected(rbft_rejected):
    check_transitions(
        rbft_rejected,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def test_tr_from_master_ordered_applied(rbft_master_ordered_applied):
    check_transitions(
        rbft_master_ordered_applied,
        RBFTReqState,
        tuple()
    )

def test_tr_from_committed(rbft_committed):
    check_transitions(
        rbft_committed,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def test_tr_from_replyed(rbft_replyed):
    check_transitions(
        rbft_replyed,
        RBFTReqState,
        (RBFTReqState.Executed,)
    )

def test_tr_from_executed_not_cleaned(rbft_executed_not_cleaned):
    assert rbft_executed_not_cleaned.state() == RBFTReqState.Executed
    check_transitions(
        rbft_executed_not_cleaned,
        RBFTReqState,
        tuple()
    )


def test_tr_from_executed_not_some_cleaned(rbft_executed_not_some_cleaned):
    assert rbft_executed_not_some_cleaned.state() == RBFTReqState.Executed
    check_transitions(
        rbft_executed_not_some_cleaned,
        RBFTReqState,
        tuple()
    )

def test_executed_and_cleaned(rbft_executed_and_cleaned):
    assert rbft_executed_and_cleaned.state() == RBFTReqState.Detached

def test_tr_from_detached(rbft_detached):
    check_transitions(
        rbft_detached,
        RBFTReqState,
        tuple()
    )


# test events
def test_propagate(rbft_propagation, operation2):
    rbftr = rbft_propagation
    sender1, sender2, sender3 = 's1', 's2', 's3'
    quorum = Quorum(2)

    good_request = rbftr.request
    # another operation should lead to different request digest
    strange_request = Request(*good_request.key, operation2)
    bad_request = Request('234', 234)

    assert not rbftr.hasPropagate(sender1)
    assert not rbftr.hasPropagate(sender2)
    assert not rbftr.finalised
    assert rbftr.votes() == 0

    rbftr.on_propagate(good_request, sender1, quorum)

    # check propagate accepted
    assert rbftr.hasPropagate(sender1)
    assert not rbftr.hasPropagate(sender2)
    assert not rbftr.finalised
    assert rbftr.votes() == 1

    # check propagate from the same sender
    with pytest.raises(ValueError) as excinfo:
        rbftr.on_propagate(good_request, sender1, quorum)
    assert ("Propagate from sender {} was alredy registered"
            .format(sender1)) in str(excinfo.value)

    # check incorrect request passed
    with pytest.raises(ValueError) as excinfo:
        rbftr.on_propagate(bad_request, sender1, quorum)
    assert ("expects requests with key {} but {} was passed"
            .format(rbftr.request.key, bad_request.key)) in str(excinfo.value)

    # check quorum reached by votes number but request is not
    # finalized due to no quorum for the same request
    rbftr.on_propagate(strange_request, sender2, quorum)
    assert not rbftr.finalised
    assert rbftr.votes() == 2

    rbftr.on_propagate(good_request, sender3, quorum)
    assert rbftr.finalised
    assert rbftr.votes() == 3

def test_forward(rbftr_all, rbft_finalized, master_inst_id, backup_inst_id):

    def check(rbftr):
        with pytest.raises(ValueError) as excinfo:
            rbftr.on_forward((backup_inst_id,))
        assert ("expects master instance id {} in passed ids"
                .format(master_inst_id)) in str(excinfo.value)

        rbftr.on_forward((master_inst_id, backup_inst_id))
        assert rbftr.state() == RBFTReqState.Forwarded
        assert rbftr.is_forwarded()
        assert tuple(rbftr.tpcRequests.keys()) == (master_inst_id, backup_inst_id)
        for inst_id in (master_inst_id, backup_inst_id):
            assert not rbftr.tpcRequests[inst_id].isReset()

    check_statefuls(
        rbftr_all,
        (rbft_finalized,),
        check)

def test_reply(rbftr_all, rbft_committed, rbft_master_rejected):

    def check(rbftr):
        rbftr.on_reply()
        assert rbftr.state() == RBFTReqState.Replyed

    check_statefuls(
        rbftr_all,
        (rbft_committed, rbft_master_rejected),
        check)

def test_execute(rbftr_all, rbft_replyed):
    def check(rbftr):
        rbftr.on_execute()
        assert rbftr.state() == RBFTReqState.Executed

    check_statefuls(
        rbftr_all,
        (rbft_replyed,),
        check)

def test_tpcevent_for_non_forwarded(rbft_propagation,
        rbft_finalized, master_inst_id):

    for rbftr in (rbft_propagation, rbft_finalized):
        with pytest.raises(RuntimeError) as excinfo:
            rbftr.on_tpcevent(master_inst_id, TPCRequest.Reject((0, 1)))
        assert ("No TPCRequest for instId {} found"
                .format(master_inst_id)) in str(excinfo.value)

def test_tpcevent_for_forwarded(rbft_forwarded,
        master_inst_id, backup_inst_id):
    rbftr = rbft_forwarded

    rbftr.on_tpcevent(backup_inst_id, TPCRequest.Reject((0, 1)))
    assert rbftr.state() == RBFTReqState.Forwarded # not changed

    rbftr.on_tpcevent(master_inst_id, TPCRequest.Reject((0, 1)))
    assert rbftr.state() == RBFTReqState.Rejected

    rbftr.on_tpcevent(backup_inst_id, TPCRequest.Reset())
    assert rbftr.state() == RBFTReqState.Rejected # not changed

    rbftr.on_reply()
    assert rbftr.state() == RBFTReqState.Replyed

    assert not rbftr.is_executed()
    rbftr.on_execute()
    assert rbftr.state() == RBFTReqState.Executed
    assert rbftr.is_executed()

    rbftr.on_tpcevent(master_inst_id, TPCRequest.Reset())
    assert rbftr.state() == RBFTReqState.Forwarded
    assert not rbftr.is_executed()

    rbftr.on_tpcevent(master_inst_id, TPCRequest.Apply())
    assert rbftr.state() == RBFTReqState.Forwarded
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Accept((0, 2)))
    assert rbftr.state() == RBFTReqState.Forwarded
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Order())
    assert rbftr.state() == RBFTReqState.Forwarded
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Commit())
    assert rbftr.state() == RBFTReqState.Committed

    for inst_id in (master_inst_id, backup_inst_id):
        rbftr.on_tpcevent(inst_id, TPCRequest.Clean())
    assert rbftr.state() == RBFTReqState.Committed # not changed

    rbftr.on_reply()
    rbftr.on_execute()
    assert rbftr.state() == RBFTReqState.Detached

def test_detached(rbft_executed_not_cleaned,
        master_inst_id, backup_inst_id):
    rbftr = rbft_executed_not_cleaned
    for inst_id in (master_inst_id, backup_inst_id):
        rbftr.on_tpcevent(inst_id, TPCRequest.Clean())
    assert rbftr.state() == RBFTReqState.Detached
