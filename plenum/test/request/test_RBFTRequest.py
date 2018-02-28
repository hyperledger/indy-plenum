import pytest

from plenum.common.request import Request
from plenum.server.quorums import Quorum
from plenum.server.propagator import (
        RBFTRequest,
        RBFTReqState
)
from plenum.test.request.helper import checkTransitions

# FIXTURES
@pytest.fixture
def rbftRequestPropagation(rbftRequest):
    return rbftRequest

@pytest.fixture
def rbftRequestFinalized(rbftRequestPropagation):
    rbftRequestPropagation.onPropagate(rbftRequestPropagation.request, 'me', Quorum(2))
    rbftRequestPropagation.onPropagate(rbftRequestPropagation.request, 'notMe', Quorum(2))
    return rbftRequestPropagation

@pytest.fixture
def rbftRequestForwarded(rbftRequestFinalized, masterInstId, backupInstId):
    rbftRequestFinalized.onForwarded((masterInstId, backupInstId))
    return rbftRequestFinalized

@pytest.fixture
def rbftRequestMasterRejected(rbftRequestForwarded, masterInstId):
    rbftRequestForwarded.onTPCRejected(masterInstId)
    return rbftRequestForwarded

@pytest.fixture
def rbftRequestRejected(rbftRequestMasterRejected):
    return rbftRequestMasterRejected

@pytest.fixture
def rbftRequestMasterOrdered(rbftRequestForwarded, masterInstId):
    rbftRequestForwarded.onTPCPp(masterInstId)
    rbftRequestForwarded.onTPCOrdered(masterInstId)
    return rbftRequestForwarded

@pytest.fixture
def rbftRequestCommitted(rbftRequestMasterOrdered):
    rbftRequestMasterOrdered.onCommitted()
    return rbftRequestMasterOrdered

@pytest.fixture
def rbftRequestReplyed(rbftRequestCommitted):
    rbftRequestCommitted.onReplyed()
    return rbftRequestCommitted

@pytest.fixture
def rbftRequestExecutedNotCleaned(rbftRequestReplyed):
    rbftRequestReplyed.onExecuted()
    return rbftRequestReplyed

@pytest.fixture
def rbftRequestExecutedNotSomeCleaned(rbftRequestExecutedNotCleaned, masterInstId):
    rbftRequestExecutedNotCleaned.onTPCCleaned(masterInstId)
    return rbftRequestExecutedNotCleaned

@pytest.fixture
def rbftRequestExecutedAndCleaned(rbftRequestExecutedNotSomeCleaned, backupInstId):
    rbftRequestExecutedNotSomeCleaned.onTPCCleaned(backupInstId)
    return rbftRequestExecutedNotSomeCleaned

@pytest.fixture
def rbftRequestDetached(rbftRequestExecutedAndCleaned):
    return rbftRequestExecutedAndCleaned


# TESTS
def testInitialState(rbftRequest):
    assert rbftRequest.state() == RBFTReqState.Propagation

def testTrFromPropagation(rbftRequestPropagation):
    checkTransitions(
        rbftRequestPropagation,
        RBFTReqState,
        (RBFTReqState.Finalized,)
    )

def testTrFromFinalized(rbftRequestFinalized):
    checkTransitions(
        rbftRequestFinalized,
        RBFTReqState,
        (RBFTReqState.Forwarded,)
    )

def testTrFromForwarded(rbftRequestForwarded):
    checkTransitions(
        rbftRequestForwarded,
        RBFTReqState,
        tuple()
    )

def testMasterRejected(rbftRequestMasterRejected):
    assert rbftRequestMasterRejected.state() == RBFTReqState.Rejected

def testTrFromRejected(rbftRequestRejected):
    checkTransitions(
        rbftRequestRejected,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def testTrFromMasterOrdered(rbftRequestMasterOrdered):
    checkTransitions(
        rbftRequestMasterOrdered,
        RBFTReqState,
        (RBFTReqState.Committed,)
    )

def testTrFromCommitted(rbftRequestCommitted):
    checkTransitions(
        rbftRequestCommitted,
        RBFTReqState,
        (RBFTReqState.Replyed,)
    )

def testTrFromReplyed(rbftRequestReplyed):
    checkTransitions(
        rbftRequestReplyed,
        RBFTReqState,
        (RBFTReqState.Executed,)
    )

def testTrFromExecutedNotCleaned(rbftRequestExecutedNotCleaned):
    assert rbftRequestExecutedNotCleaned.state() == RBFTReqState.Executed
    checkTransitions(
        rbftRequestExecutedNotCleaned,
        RBFTReqState,
        tuple()
    )


def testTrFromExecutedNotSomeCleaned(rbftRequestExecutedNotSomeCleaned):
    assert rbftRequestExecutedNotSomeCleaned.state() == RBFTReqState.Executed
    checkTransitions(
        rbftRequestExecutedNotSomeCleaned,
        RBFTReqState,
        tuple()
    )

def testExecutedAndCleaned(rbftRequestExecutedAndCleaned):
    assert rbftRequestExecutedAndCleaned.state() == RBFTReqState.Detached

def testTrFromDetached(rbftRequestDetached):
    checkTransitions(
        rbftRequestDetached,
        RBFTReqState,
        tuple()
    )


def testOnPropagate(rbftRequest, operation2):
    sender1, sender2, sender3 = 's1', 's2', 's3'
    quorum = Quorum(2)

    goodRequest = rbftRequest.request
    # another operation should lead to different request digest
    strangeRequest = Request(*goodRequest.key, operation2)
    badRequest = Request('234', 234)

    assert not rbftRequest.hasPropagate(sender1)
    assert not rbftRequest.hasPropagate(sender2)
    assert not rbftRequest.finalised
    assert rbftRequest.votes() == 0

    rbftRequest.onPropagate(goodRequest, sender1, quorum)

    # check propagate accepted
    assert rbftRequest.hasPropagate(sender1)
    assert not rbftRequest.hasPropagate(sender2)
    assert not rbftRequest.finalised
    assert rbftRequest.votes() == 1

    # check ignoring propagate from the same sender
    rbftRequest.onPropagate(goodRequest, sender1, quorum)
    assert not rbftRequest.finalised
    assert rbftRequest.votes() == 1

    # check quorum reached by votes number but request is not
    # finalized due to no quorum for the same request
    rbftRequest.onPropagate(strangeRequest, sender2, quorum)
    assert not rbftRequest.finalised
    assert rbftRequest.votes() == 2

    rbftRequest.onPropagate(goodRequest, sender3, quorum)
    assert rbftRequest.finalised
    assert rbftRequest.votes() == 3

    # check assertion raised
    with pytest.raises(AssertionError):
        rbftRequest.onPropagate(badRequest, sender1, quorum)
