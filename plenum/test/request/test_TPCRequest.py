import pytest

from plenum.server.propagator import (
        TPCRequest,
        TPCReqState
)
from plenum.test.request.helper import checkTransitions

# FIXTURES
@pytest.fixture
def tpcRequest(rbftRequest):
    return TPCRequest(rbftRequest=rbftRequest, instId=0)

@pytest.fixture
def tpcRequestForwarded(tpcRequest):
    return tpcRequest

@pytest.fixture
def tpcRequestRejected(tpcRequestForwarded):
    tpcRequestForwarded.onRejected()
    return tpcRequestForwarded

@pytest.fixture
def tpcRequestIn3PC(tpcRequestForwarded):
    tpcRequestForwarded.onPP()
    return tpcRequestForwarded

@pytest.fixture
def tpcRequestOrdered(tpcRequestIn3PC):
    tpcRequestIn3PC.onOrdered()
    return tpcRequestIn3PC

@pytest.fixture
def tpcRequestCleaned(tpcRequestForwarded):
    tpcRequestForwarded.onCleaned()
    return tpcRequestForwarded

# TESTS
def testInitialState(tpcRequest):
    assert tpcRequest.state() == TPCReqState.Forwarded

def testTrFromForwarded(tpcRequestForwarded):
    checkTransitions(
        tpcRequestForwarded,
        TPCReqState,
        (TPCReqState.Rejected, TPCReqState.In3PC, TPCReqState.Cleaned)
    )

def testTrFromRejected(tpcRequestRejected):
    checkTransitions(
        tpcRequestRejected,
        TPCReqState,
        (TPCReqState.Cleaned,)
    )

def testTrFromIn3PC(tpcRequestIn3PC):
    checkTransitions(
        tpcRequestIn3PC,
        TPCReqState,
        (TPCReqState.Ordered, TPCReqState.Cleaned)
    )

def testTrFromOrdered(tpcRequestOrdered):
    checkTransitions(
        tpcRequestOrdered,
        TPCReqState,
        (TPCReqState.Cleaned,)
    )

def testTrFromCleaned(tpcRequestCleaned):
    checkTransitions(
        tpcRequestCleaned,
        TPCReqState,
        tuple()
    )

# test events
def testOnPP(tpcRequestForwarded):
    tpcRequestForwarded.onPP()
    assert tpcRequestForwarded.state() == TPCReqState.In3PC

def testOnOrdered(tpcRequestIn3PC):
    tpcRequestIn3PC.onOrdered()
    assert tpcRequestIn3PC.state() == TPCReqState.Ordered

def testOnRejected(tpcRequestForwarded):
    tpcRequestForwarded.onRejected()
    assert tpcRequestForwarded.state() == TPCReqState.Rejected

def testOnCleaned(tpcRequestIn3PC):
    tpcRequestIn3PC.onCleaned()
    assert tpcRequestIn3PC.state() == TPCReqState.Cleaned
