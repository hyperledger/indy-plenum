import pytest
from copy import deepcopy

from plenum.common.request import Request
from plenum.server.quorums import Quorum
from plenum.server.tpcrequest import TPCRequest
from plenum.server.rbftrequest import (
        RBFTReqState
)
from plenum.test.request.helper import check_transitions

# TODO tests for:
#   - onTPC... events
#   - tryTPCState
#   - onForward
#   - onExecuted
#   - and other API

# FIXTURES
@pytest.fixture
def rbft_request_propagation(rbft_request):
    return deepcopy(rbft_request)

@pytest.fixture
def rbft_request_finalized(rbft_request_propagation):
    rbftr = deepcopy(rbft_request_propagation)
    rbftr.on_propagate(rbftr.request, 'me', Quorum(2))
    rbftr.on_propagate(rbftr.request, 'not_me', Quorum(2))
    return rbftr

@pytest.fixture
def rbft_request_forwarded(rbft_request_finalized, master_inst_id, backup_inst_id):
    rbftr = deepcopy(rbft_request_finalized)
    rbftr.on_forward((master_inst_id, backup_inst_id))
    return rbftr

@pytest.fixture
def rbft_request_master_rejected(rbft_request_forwarded, master_inst_id):
    rbftr = deepcopy(rbft_request_forwarded)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Reject((0, 1)))
    return rbftr

@pytest.fixture
def rbft_request_rejected(rbft_request_master_rejected):
    rbftr = deepcopy(rbft_request_master_rejected)
    return rbft_request_master_rejected

@pytest.fixture
def rbft_request_master_ordered_applied(rbft_request_forwarded, master_inst_id):
    rbftr = deepcopy(rbft_request_forwarded)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Accept((0, 1)))
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Order())
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Apply())
    return rbftr

@pytest.fixture
def rbft_request_committed(rbft_request_master_ordered_applied, master_inst_id):
    rbftr = deepcopy(rbft_request_master_ordered_applied)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Commit())
    return rbftr

@pytest.fixture
def rbft_request_replyed(rbft_request_committed):
    rbftr = deepcopy(rbft_request_committed)
    rbftr.on_reply()
    return rbftr

@pytest.fixture
def rbft_request_executed_not_cleaned(rbft_request_replyed):
    rbftr = deepcopy(rbft_request_replyed)
    rbftr.on_execute()
    return rbftr

@pytest.fixture
def rbft_request_executed_not_some_cleaned(rbft_request_executed_not_cleaned, master_inst_id):
    rbftr = deepcopy(rbft_request_executed_not_cleaned)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Clean())
    return rbftr

@pytest.fixture
def rbft_request_executed_and_cleaned(rbft_request_executed_not_some_cleaned, backup_inst_id):
    rbftr = deepcopy(rbft_request_executed_not_some_cleaned)
    rbftr.on_tpcevent(backup_inst_id, TPCRequest.Clean())
    return rbftr

@pytest.fixture
def rbft_request_detached(rbft_request_executed_and_cleaned):
    return deepcopy(rbft_request_executed_and_cleaned)


# TESTS
def test_initial_state(rbft_request):
    assert rbft_request.state() == RBFTReqState.Propagation

def test_tr_from_propagation(rbft_request_propagation):
    check_transitions(
        rbft_request_propagation,
        RBFTReqState,
        (RBFTReqState.Finalized,)
    )

def test_tr_from_finalized(rbft_request_finalized):
    check_transitions(
        rbft_request_finalized,
        RBFTReqState,
        (RBFTReqState.Forwarded,)
    )

def test_tr_from_forwarded(rbft_request_forwarded):
    check_transitions(
        rbft_request_forwarded,
        RBFTReqState,
        (RBFTReqState.Forwarded,)
    )

def test_master_rejected(rbft_request_master_rejected):
    assert rbft_request_master_rejected.state() == RBFTReqState.Rejected

def test_tr_from_rejected(rbft_request_rejected):
    check_transitions(
        rbft_request_rejected,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def test_tr_from_master_ordered_applied(rbft_request_master_ordered_applied):
    check_transitions(
        rbft_request_master_ordered_applied,
        RBFTReqState,
        tuple()
    )

def test_tr_from_committed(rbft_request_committed):
    check_transitions(
        rbft_request_committed,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def test_tr_from_replyed(rbft_request_replyed):
    check_transitions(
        rbft_request_replyed,
        RBFTReqState,
        (RBFTReqState.Executed,)
    )

def test_tr_from_executed_not_cleaned(rbft_request_executed_not_cleaned):
    assert rbft_request_executed_not_cleaned.state() == RBFTReqState.Executed
    check_transitions(
        rbft_request_executed_not_cleaned,
        RBFTReqState,
        tuple()
    )


def test_tr_from_executed_not_some_cleaned(rbft_request_executed_not_some_cleaned):
    assert rbft_request_executed_not_some_cleaned.state() == RBFTReqState.Executed
    check_transitions(
        rbft_request_executed_not_some_cleaned,
        RBFTReqState,
        tuple()
    )

def test_executed_and_cleaned(rbft_request_executed_and_cleaned):
    assert rbft_request_executed_and_cleaned.state() == RBFTReqState.Detached

def test_tr_from_detached(rbft_request_detached):
    check_transitions(
        rbft_request_detached,
        RBFTReqState,
        tuple()
    )


def test_on_propagate(rbft_request, operation2):
    sender1, sender2, sender3 = 's1', 's2', 's3'
    quorum = Quorum(2)

    good_request = rbft_request.request
    # another operation should lead to different request digest
    strange_request = Request(*good_request.key, operation2)
    bad_request = Request('234', 234)

    assert not rbft_request.hasPropagate(sender1)
    assert not rbft_request.hasPropagate(sender2)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 0

    rbft_request.on_propagate(good_request, sender1, quorum)

    # check propagate accepted
    assert rbft_request.hasPropagate(sender1)
    assert not rbft_request.hasPropagate(sender2)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 1

    # check ignoring propagate from the same sender
    rbft_request.on_propagate(good_request, sender1, quorum)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 1

    # check quorum reached by votes number but request is not
    # finalized due to no quorum for the same request
    rbft_request.on_propagate(strange_request, sender2, quorum)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 2

    rbft_request.on_propagate(good_request, sender3, quorum)
    assert rbft_request.finalised
    assert rbft_request.votes() == 3

    # check assertion raised
    with pytest.raises(AssertionError):
        rbft_request.on_propagate(bad_request, sender1, quorum)
