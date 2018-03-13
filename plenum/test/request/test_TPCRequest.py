import pytest
from copy import deepcopy
from collections import OrderedDict

from plenum.server.tpcrequest import (
        TPCRequest,
        TPCReqState,
        TransactionState
)
from plenum.server.stateful import TransitionError

from plenum.test.request.helper import check_transitions, check_statefuls

# FIXTURES
@pytest.fixture
def tpc_request(rbft_request):
    return TPCRequest(rbftRequest=rbft_request, instId=0)

@pytest.fixture
def tpcr_forwarded(tpc_request):
    return deepcopy(tpc_request)

@pytest.fixture
def tpcr_forwarded_applied(tpc_request):
    tpcr = deepcopy(tpc_request)
    tpcr.on_apply()
    return tpcr

@pytest.fixture
def tpcr_in_3pc(tpcr_forwarded):
    tpcr = deepcopy(tpcr_forwarded)
    tpcr.on_accept((0, 1))
    return tpcr

@pytest.fixture
def tpcr_in_3pc_applied(tpcr_forwarded_applied):
    tpcr = deepcopy(tpcr_forwarded_applied)
    tpcr.on_accept((0, 1))
    return tpcr

@pytest.fixture
def tpcr_ordered(tpcr_in_3pc):
    tpcr = deepcopy(tpcr_in_3pc)
    tpcr.on_order()
    return tpcr

@pytest.fixture
def tpcr_ordered_applied(tpcr_in_3pc_applied):
    tpcr = deepcopy(tpcr_in_3pc_applied)
    tpcr.on_order()
    return tpcr

@pytest.fixture
def tpcr_committed(tpcr_ordered_applied):
    tpcr = deepcopy(tpcr_ordered_applied)
    tpcr.on_commit()
    return tpcr

@pytest.fixture
def tpcr_rejected(tpcr_forwarded):
    tpcr = deepcopy(tpcr_forwarded)
    tpcr.on_reject((0, 1))
    return tpcr

@pytest.fixture
def tpcr_cancelled(tpcr_forwarded):
    tpcr = deepcopy(tpcr_forwarded)
    tpcr.on_cancel()
    return tpcr

@pytest.fixture
def tpcr_cleaned_rejected(tpcr_rejected):
    tpcr = deepcopy(tpcr_rejected)
    tpcr.on_clean()
    return tpcr

@pytest.fixture
def tpcr_cleaned_cancelled(tpcr_cancelled):
    tpcr = deepcopy(tpcr_cancelled)
    tpcr.on_clean()
    return tpcr

@pytest.fixture
def tpcr_cleaned_committed(tpcr_committed):
    tpcr = deepcopy(tpcr_committed)
    tpcr.on_clean()
    return tpcr

@pytest.fixture
def tpcr_cleaned_not_applied(tpcr_ordered):
    tpcr = deepcopy(tpcr_ordered)
    tpcr.on_clean()
    return tpcr

@pytest.fixture
def tpcr_all(
        tpcr_forwarded,
        tpcr_forwarded_applied,
        tpcr_in_3pc,
        tpcr_in_3pc_applied,
        tpcr_ordered,
        tpcr_ordered_applied,
        tpcr_committed,
        tpcr_rejected,
        tpcr_cancelled,
        tpcr_cleaned_rejected,
        tpcr_cleaned_cancelled,
        tpcr_cleaned_committed,
        tpcr_cleaned_not_applied):
    return (tpcr_forwarded,
            tpcr_forwarded_applied,
            tpcr_in_3pc,
            tpcr_in_3pc_applied,
            tpcr_ordered,
            tpcr_ordered_applied,
            tpcr_committed,
            tpcr_rejected,
            tpcr_cancelled,
            tpcr_cleaned_rejected,
            tpcr_cleaned_cancelled,
            tpcr_cleaned_committed,
            tpcr_cleaned_not_applied)

# TESTS
def test_initial_state(tpc_request):
    assert tpc_request.state() == TPCReqState.Forwarded
    assert tpc_request.txnState() == TransactionState.NotApplied

def test_tr_from_forwarded(tpcr_forwarded):
    assert tpcr_forwarded.isReset()
    check_transitions(
        tpcr_forwarded,
        TPCReqState,
        (TPCReqState.Rejected,
         TPCReqState.In3PC,
         TPCReqState.Cancelled,
         TPCReqState.Cleaned)
    )

def test_tr_from_forwarded_applied(tpcr_forwarded_applied):
    check_transitions(
        tpcr_forwarded_applied,
        TPCReqState,
        (TPCReqState.In3PC,)
    )

def test_tr_from_in_3pc(tpcr_in_3pc):
    check_transitions(
        tpcr_in_3pc,
        TPCReqState,
        (TPCReqState.Forwarded,
         TPCReqState.Ordered,
         TPCReqState.Cancelled,
         TPCReqState.Cleaned)
    )

def test_tr_from_in_3pc_applied(tpcr_in_3pc_applied):
    check_transitions(
        tpcr_in_3pc_applied,
        TPCReqState,
        (TPCReqState.Ordered,)
    )

def test_tr_from_ordered(tpcr_ordered):
    check_transitions(
        tpcr_ordered,
        TPCReqState,
        (TPCReqState.Forwarded, TPCReqState.Cancelled, TPCReqState.Cleaned)
    )

def test_tr_from_ordered_applied(tpcr_ordered_applied):
    check_transitions(
        tpcr_ordered_applied,
        TPCReqState,
        tuple()
    )

def test_tr_from_committed(tpcr_committed):
    check_transitions(
        tpcr_committed,
        TPCReqState,
        (TPCReqState.Cleaned,)
    )

def test_tr_from_rejected(tpcr_rejected):
    check_transitions(
        tpcr_rejected,
        TPCReqState,
        (TPCReqState.Forwarded, TPCReqState.Cleaned)
    )

def test_tr_from_cancelled(tpcr_cancelled):
    check_transitions(
        tpcr_cancelled,
        TPCReqState,
        (TPCReqState.Cleaned,)
    )

def test_tr_from_cleaned(
        tpcr_cleaned_rejected,
        tpcr_cleaned_cancelled,
        tpcr_cleaned_committed,
        tpcr_cleaned_not_applied):
    check_transitions(
        tpcr_cleaned_rejected,
        TPCReqState,
        (TPCReqState.Forwarded,)
    )
    check_transitions(
        tpcr_cleaned_cancelled,
        TPCReqState,
        tuple()
    )
    check_transitions(
        tpcr_cleaned_committed,
        TPCReqState,
        tuple()
    )
    check_transitions(
        tpcr_cleaned_not_applied,
        TPCReqState,
        (TPCReqState.Forwarded,)
    )

# test events
def test_apply(tpcr_all, tpcr_forwarded, tpcr_in_3pc, tpcr_ordered):
    def check(tpcr):
        tpcr.on_apply()
        assert tpcr.txnState() == TransactionState.Applied
        assert tpcr.isApplied()
        assert not tpcr.isCommitted()

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded, tpcr_in_3pc, tpcr_ordered),
        check)

def test_commit(tpcr_all, tpcr_ordered_applied):
    def check(tpcr):
        tpcr.on_commit()
        assert tpcr.txnState() == TransactionState.Committed
        assert tpcr.isApplied()
        assert tpcr.isCommitted()

    check_statefuls(
        tpcr_all,
        (tpcr_ordered_applied,),
        check)

def test_revert(tpcr_all,
                tpcr_forwarded_applied,
                tpcr_in_3pc_applied,
                tpcr_ordered_applied):

    def check(tpcr):
        tpcr.on_revert()
        assert tpcr.txnState() == TransactionState.NotApplied
        assert not tpcr.isApplied()
        assert not tpcr.isCommitted()

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded_applied,
         tpcr_in_3pc_applied,
         tpcr_ordered_applied),
        check)

def test_accept(tpcr_all,
                tpcr_forwarded,
                tpcr_forwarded_applied):
    def check(tpcr):
        tpcKey = (0, 1)
        txnState = tpcr.txnState()
        tpcr.on_accept(tpcKey)
        assert tpcr.tpcKey == tpcKey
        assert tpcr.state() == TPCReqState.In3PC
        assert tpcr.txnState() == txnState

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded,
         tpcr_forwarded_applied),
        check)

def test_reject(tpcr_all, tpcr_forwarded):
    def check(tpcr):
        tpcKey = (0, 1)
        txnState = tpcr.txnState()
        tpcr.on_reject(tpcKey)
        assert tpcr.tpcKey == tpcKey
        assert tpcr.state() == TPCReqState.Rejected
        assert tpcr.txnState() == txnState
        assert tpcr.isRejected()

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded,),
        check)

def test_order(tpcr_all,
                tpcr_in_3pc,
                tpcr_in_3pc_applied):
    def check(tpcr):
        tpcr.on_order()
        assert tpcr.state() == TPCReqState.Ordered

    check_statefuls(
        tpcr_all,
        (tpcr_in_3pc,
        tpcr_in_3pc_applied),
        check)

def test_cancel(tpcr_all,
                tpcr_forwarded,
                tpcr_in_3pc,
                tpcr_ordered):

    def check(tpcr):
        tpcr.on_cancel()
        assert tpcr.state() == TPCReqState.Cancelled

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded, tpcr_in_3pc, tpcr_ordered),
        check)

def test_clean(tpcr_all,
                tpcr_forwarded,
                tpcr_in_3pc,
                tpcr_ordered,
                tpcr_committed,
                tpcr_rejected,
                tpcr_cancelled):
    def check(tpcr):
        tpcr.on_clean()
        assert tpcr.state() == TPCReqState.Cleaned
        assert tpcr.isCleaned()

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded,
         tpcr_in_3pc,
         tpcr_ordered,
         tpcr_committed,
         tpcr_rejected,
         tpcr_cancelled),
        check)

def test_reset(tpcr_all,
                tpcr_in_3pc,
                tpcr_ordered,
                tpcr_rejected,
                tpcr_cleaned_rejected,
                tpcr_cleaned_not_applied):

    def check(tpcr):
        tpcKey = tpcr.tpcKey
        states = list(tpcr.states)
        tpcr.on_reset()
        assert tpcr.state() == TPCReqState.Forwarded
        assert tpcr.isReset()
        assert tpcr.tpcKey is None
        assert tpcr.states == [TPCReqState.Forwarded]
        assert tpcr.old_rounds == OrderedDict({tpcKey: tuple(states + [TPCReqState.Forwarded])})

    check_statefuls(
        tpcr_all,
        (tpcr_in_3pc,
         tpcr_ordered,
         tpcr_rejected,
         tpcr_cleaned_rejected,
         tpcr_cleaned_not_applied),
        check)
