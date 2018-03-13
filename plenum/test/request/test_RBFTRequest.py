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
def rbftRequestPropagation(rbft_request):
    return deepcopy(rbft_request)

@pytest.fixture
def rbftRequestFinalized(rbftRequestPropagation):
    rbftr = deepcopy(rbftRequestPropagation)
    rbftr.on_propagate(rbftr.request, 'me', Quorum(2))
    rbftr.on_propagate(rbftr.request, 'notMe', Quorum(2))
    return rbftr

@pytest.fixture
def rbftRequestForwarded(rbftRequestFinalized, master_inst_id, backup_inst_id):
    rbftr = deepcopy(rbftRequestFinalized)
    rbftr.on_forward((master_inst_id, backup_inst_id))
    return rbftr

@pytest.fixture
def rbftRequestMasterRejected(rbftRequestForwarded, master_inst_id):
    rbftr = deepcopy(rbftRequestForwarded)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Reject((0, 1)))
    return rbftr

@pytest.fixture
def rbftRequestRejected(rbftRequestMasterRejected):
    rbftr = deepcopy(rbftRequestMasterRejected)
    return rbftRequestMasterRejected

@pytest.fixture
def rbftRequestMasterOrderedApplied(rbftRequestForwarded, master_inst_id):
    rbftr = deepcopy(rbftRequestForwarded)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Accept((0, 1)))
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Order())
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Apply())
    return rbftr

@pytest.fixture
def rbftRequestCommitted(rbftRequestMasterOrderedApplied, master_inst_id):
    rbftr = deepcopy(rbftRequestMasterOrderedApplied)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Commit())
    return rbftr

@pytest.fixture
def rbftRequestReplyed(rbftRequestCommitted):
    rbftr = deepcopy(rbftRequestCommitted)
    rbftr.on_reply()
    return rbftr

@pytest.fixture
def rbftRequestExecutedNotCleaned(rbftRequestReplyed):
    rbftr = deepcopy(rbftRequestReplyed)
    rbftr.on_execute()
    return rbftr

@pytest.fixture
def rbftRequestExecutedNotSomeCleaned(rbftRequestExecutedNotCleaned, master_inst_id):
    rbftr = deepcopy(rbftRequestExecutedNotCleaned)
    rbftr.on_tpcevent(master_inst_id, TPCRequest.Clean())
    return rbftr

@pytest.fixture
def rbftRequestExecutedAndCleaned(rbftRequestExecutedNotSomeCleaned, backup_inst_id):
    rbftr = deepcopy(rbftRequestExecutedNotSomeCleaned)
    rbftr.on_tpcevent(backup_inst_id, TPCRequest.Clean())
    return rbftr

@pytest.fixture
def rbftRequestDetached(rbftRequestExecutedAndCleaned):
    return deepcopy(rbftRequestExecutedAndCleaned)


# TESTS
def testInitialState(rbft_request):
    assert rbft_request.state() == RBFTReqState.Propagation

def testTrFromPropagation(rbftRequestPropagation):
    check_transitions(
        rbftRequestPropagation,
        RBFTReqState,
        (RBFTReqState.Finalized,)
    )

def testTrFromFinalized(rbftRequestFinalized):
    check_transitions(
        rbftRequestFinalized,
        RBFTReqState,
        (RBFTReqState.Forwarded,)
    )

def testTrFromForwarded(rbftRequestForwarded):
    check_transitions(
        rbftRequestForwarded,
        RBFTReqState,
        (RBFTReqState.Forwarded,)
    )

def testMasterRejected(rbftRequestMasterRejected):
    assert rbftRequestMasterRejected.state() == RBFTReqState.Rejected

def testTrFromRejected(rbftRequestRejected):
    check_transitions(
        rbftRequestRejected,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def testTrFromMasterOrderedApplied(rbftRequestMasterOrderedApplied):
    check_transitions(
        rbftRequestMasterOrderedApplied,
        RBFTReqState,
        tuple()
    )

def testTrFromCommitted(rbftRequestCommitted):
    check_transitions(
        rbftRequestCommitted,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def testTrFromReplyed(rbftRequestReplyed):
    check_transitions(
        rbftRequestReplyed,
        RBFTReqState,
        (RBFTReqState.Executed,)
    )

def testTrFromExecutedNotCleaned(rbftRequestExecutedNotCleaned):
    assert rbftRequestExecutedNotCleaned.state() == RBFTReqState.Executed
    check_transitions(
        rbftRequestExecutedNotCleaned,
        RBFTReqState,
        tuple()
    )


def testTrFromExecutedNotSomeCleaned(rbftRequestExecutedNotSomeCleaned):
    assert rbftRequestExecutedNotSomeCleaned.state() == RBFTReqState.Executed
    check_transitions(
        rbftRequestExecutedNotSomeCleaned,
        RBFTReqState,
        tuple()
    )

def testExecutedAndCleaned(rbftRequestExecutedAndCleaned):
    assert rbftRequestExecutedAndCleaned.state() == RBFTReqState.Detached

def testTrFromDetached(rbftRequestDetached):
    check_transitions(
        rbftRequestDetached,
        RBFTReqState,
        tuple()
    )


def testOnPropagate(rbft_request, operation2):
    sender1, sender2, sender3 = 's1', 's2', 's3'
    quorum = Quorum(2)

    goodRequest = rbft_request.request
    # another operation should lead to different request digest
    strangeRequest = Request(*goodRequest.key, operation2)
    badRequest = Request('234', 234)

    assert not rbft_request.hasPropagate(sender1)
    assert not rbft_request.hasPropagate(sender2)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 0

    rbft_request.on_propagate(goodRequest, sender1, quorum)

    # check propagate accepted
    assert rbft_request.hasPropagate(sender1)
    assert not rbft_request.hasPropagate(sender2)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 1

    # check ignoring propagate from the same sender
    rbft_request.on_propagate(goodRequest, sender1, quorum)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 1

    # check quorum reached by votes number but request is not
    # finalized due to no quorum for the same request
    rbft_request.on_propagate(strangeRequest, sender2, quorum)
    assert not rbft_request.finalised
    assert rbft_request.votes() == 2

    rbft_request.on_propagate(goodRequest, sender3, quorum)
    assert rbft_request.finalised
    assert rbft_request.votes() == 3

    # check assertion raised
    with pytest.raises(AssertionError):
        rbft_request.on_propagate(badRequest, sender1, quorum)
