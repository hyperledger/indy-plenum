import pytest
from copy import deepcopy
from collections import OrderedDict

from plenum.server.rbftrequest import (
        TPCRequest,
        TPCReqState,
        TransactionState
)
from plenum.server.stateful import TransitionError

from plenum.test.request.helper import checkTransitions

# FIXTURES
@pytest.fixture
def tpcRequest(rbftRequest):
    return TPCRequest(rbftRequest=rbftRequest, instId=0)

@pytest.fixture
def tpcRForwarded(tpcRequest):
    return deepcopy(tpcRequest)

@pytest.fixture
def tpcRForwardedApplied(tpcRequest):
    tpcR = deepcopy(tpcRequest)
    tpcR.onApply()
    return tpcR

@pytest.fixture
def tpcRIn3PC(tpcRForwarded):
    tpcR = deepcopy(tpcRForwarded)
    tpcR.onPP((0, 1), True)
    return tpcR

@pytest.fixture
def tpcRIn3PCApplied(tpcRForwardedApplied):
    tpcR = deepcopy(tpcRForwardedApplied)
    tpcR.onPP((0, 1), True)
    return tpcR

@pytest.fixture
def tpcROrdered(tpcRIn3PC):
    tpcR = deepcopy(tpcRIn3PC)
    tpcR.onOrder()
    return tpcR

@pytest.fixture
def tpcROrderedApplied(tpcRIn3PCApplied):
    tpcR = deepcopy(tpcRIn3PCApplied)
    tpcR.onOrder()
    return tpcR

@pytest.fixture
def tpcRCommitted(tpcROrderedApplied):
    tpcR = deepcopy(tpcROrderedApplied)
    tpcR.onCommit()
    return tpcR

@pytest.fixture
def tpcRRejected(tpcRForwarded):
    tpcR = deepcopy(tpcRForwarded)
    tpcR.onPP((0, 1), False)
    return tpcR

@pytest.fixture
def tpcRCancelled(tpcRForwarded):
    tpcR = deepcopy(tpcRForwarded)
    tpcR.onCancel()
    return tpcR

@pytest.fixture
def tpcRCleaned(tpcRRejected):
    tpcR = deepcopy(tpcRRejected)
    tpcR.onClean()
    return tpcR

# TESTS
def testInitialState(tpcRequest):
    assert tpcRequest.state() == TPCReqState.Forwarded
    assert tpcRequest.txnState() == TransactionState.NotApplied

def testTrFromForwarded(tpcRForwarded):
    checkTransitions(
        tpcRForwarded,
        TPCReqState,
        (TPCReqState.Rejected,
         TPCReqState.In3PC,
         TPCReqState.Cancelled,
         TPCReqState.Cleaned)
    )

def testTrFromForwardedApplied(tpcRForwardedApplied):
    checkTransitions(
        tpcRForwardedApplied,
        TPCReqState,
        (TPCReqState.In3PC,)
    )

def testTrFromIn3PC(tpcRIn3PC):
    checkTransitions(
        tpcRIn3PC,
        TPCReqState,
        (TPCReqState.Forwarded,
         TPCReqState.Ordered,
         TPCReqState.Cancelled,
         TPCReqState.Cleaned)
    )

def testTrFromIn3PCApplied(tpcRIn3PCApplied):
    checkTransitions(
        tpcRIn3PCApplied,
        TPCReqState,
        (TPCReqState.Ordered,)
    )

def testTrFromOrdered(tpcROrdered):
    checkTransitions(
        tpcROrdered,
        TPCReqState,
        (TPCReqState.Cancelled, TPCReqState.Cleaned)
    )

def testTrFromOrderedApplied(tpcROrderedApplied):
    checkTransitions(
        tpcROrderedApplied,
        TPCReqState,
        tuple()
    )

def testTrFromCommitted(tpcRCommitted):
    checkTransitions(
        tpcRCommitted,
        TPCReqState,
        (TPCReqState.Cleaned,)
    )

def testTrFromRejected(tpcRRejected):
    checkTransitions(
        tpcRRejected,
        TPCReqState,
        (TPCReqState.Forwarded, TPCReqState.Cleaned)
    )

def testTrFromCancelled(tpcRCancelled):
    checkTransitions(
        tpcRCancelled,
        TPCReqState,
        (TPCReqState.Cleaned,)
    )

def testTrFromCleaned(tpcRCleaned):
    checkTransitions(
        tpcRCleaned,
        TPCReqState,
        tuple()
    )

# test events
def testOnApply(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    for tpcR in (
            tpcRForwarded,
            tpcRIn3PC,
            tpcROrdered):
        tpcR.onApply()
        assert tpcR.txnState() == TransactionState.Applied
        assert tpcR.isApplied()
        assert not tpcR.isCommitted()

    for tpcR in (
            tpcRForwardedApplied,
            tpcRIn3PCApplied,
            tpcROrderedApplied,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onApply()


def testOnCommit(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    for tpcR in (tpcROrderedApplied,):
        tpcR.onCommit()
        assert tpcR.txnState() == TransactionState.Committed
        assert not tpcR.isApplied()
        assert tpcR.isCommitted()

    for tpcR in (
            tpcRForwarded,
            tpcRForwardedApplied,
            tpcRIn3PC,
            tpcRIn3PCApplied,
            tpcROrdered,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onCommit()

def testOnRevert(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    for tpcR in (
            tpcRForwardedApplied,
            tpcRIn3PCApplied,
            tpcROrderedApplied):
        tpcR.onRevert()
        assert tpcR.txnState() == TransactionState.NotApplied
        assert not tpcR.isApplied()
        assert not tpcR.isCommitted()

    for tpcR in (
            tpcRForwarded,
            tpcRIn3PC,
            tpcROrdered,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onRevert()


def testOnPP(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    tpcKey = (0, 1)

    for tpcR in (
            tpcRForwarded,
            tpcRForwardedApplied):
        txnState = tpcR.txnState()
        tpcR.onPP(tpcKey, True)
        assert tpcR.tpcKey == tpcKey
        assert tpcR.state() == TPCReqState.In3PC
        assert tpcR.txnState() == txnState

    for tpcR in (
            tpcRIn3PC,
            tpcRIn3PCApplied,
            tpcROrdered,
            tpcROrderedApplied,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onPP(tpcKey, True)

def testOnPPRejected(tpcRForwarded,
                     tpcRForwardedApplied,
                     tpcRIn3PC,
                     tpcRIn3PCApplied,
                     tpcROrdered,
                     tpcROrderedApplied,
                     tpcRCommitted,
                     tpcRRejected,
                     tpcRCancelled,
                     tpcRCleaned):

    tpcKey = (0, 1)

    for tpcR in (
            tpcRForwarded,):
        txnState = tpcR.txnState()
        tpcR.onPP(tpcKey, False)
        assert tpcR.tpcKey == tpcKey
        assert tpcR.state() == TPCReqState.Rejected
        assert tpcR.txnState() == txnState

    for tpcR in (
            tpcRForwardedApplied,
            tpcRIn3PC,
            tpcRIn3PCApplied,
            tpcROrdered,
            tpcROrderedApplied,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onPP(tpcKey, False)

def testOnOrder(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    for tpcR in (
            tpcRIn3PC,
            tpcRIn3PCApplied):
        tpcR.onOrder()
        assert tpcR.state() == TPCReqState.Ordered

    for tpcR in (
            tpcRForwarded,
            tpcRForwardedApplied,
            tpcROrdered,
            tpcROrderedApplied,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onOrder()

def testOnCancel(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    for tpcR in (
            tpcRForwarded,
            tpcRIn3PC,
            tpcROrdered):
        tpcR.onCancel()
        assert tpcR.state() == TPCReqState.Cancelled

    for tpcR in (
            tpcRForwardedApplied,
            tpcRIn3PCApplied,
            tpcROrderedApplied,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onCancel()

def testOnClean(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    for tpcR in (
            tpcRForwarded,
            tpcRIn3PC,
            tpcROrdered,
            tpcRCommitted,
            tpcRRejected,
            tpcRCancelled):
        tpcR.onClean()
        assert tpcR.state() == TPCReqState.Cleaned

    for tpcR in (
            tpcRForwardedApplied,
            tpcRIn3PCApplied,
            tpcROrderedApplied,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onClean()

def testOnReset(tpcRForwarded,
                tpcRForwardedApplied,
                tpcRIn3PC,
                tpcRIn3PCApplied,
                tpcROrdered,
                tpcROrderedApplied,
                tpcRCommitted,
                tpcRRejected,
                tpcRCancelled,
                tpcRCleaned):

    for tpcR in (
            tpcRIn3PC,
            tpcRRejected):
        tpcKey = tuple(tpcR.tpcKey)
        states = list(tpcR.states)
        tpcR.onReset()
        assert tpcR.state() == TPCReqState.Forwarded
        assert tpcR.tpcKey is None
        assert tpcR.states == [TPCReqState.Forwarded]
        assert tpcR.old_rounds == OrderedDict({tpcKey: states})

    for tpcR in (
            tpcRForwarded,
            tpcRForwardedApplied,
            tpcRIn3PCApplied,
            tpcROrdered,
            tpcROrderedApplied,
            tpcRCommitted,
            tpcRCancelled,
            tpcRCleaned):
        with pytest.raises(TransitionError):
            tpcR.onReset()
