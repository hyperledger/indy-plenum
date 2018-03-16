import pytest
from copy import deepcopy
from collections import OrderedDict

from plenum.server.tpcrequest import (
        TPCRequest,
        TPCReqState,
        TxnState
)
from plenum.server.stateful import TransitionError

from plenum.test.request.helper import check_transitions, check_statefuls

# TODO
# - dry for all events
#   - separate tests for each get-API (e.g. is_... methods)

# FIXTURES
@pytest.fixture
def tpc_request(rbft_request):
    return TPCRequest(rbft_request=rbft_request, inst_id=0)

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
    assert tpc_request.txn_state() == TxnState.Shallow

def test_tr_from_forwarded(tpcr_forwarded):
    assert not tpcr_forwarded.is_reset()
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
def test_TPCReqAccept():
    with pytest.raises(ValueError) as excinfo:
        TPCRequest.Accept(None)
    assert "tpc_key should be defined" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        TPCRequest.Accept(None)
    assert "tpc_key should be defined" in str(excinfo.value)

def test_TPCReqReject():
    with pytest.raises(ValueError) as excinfo:
        TPCRequest.Reject(None)
    assert "tpc_key should be defined" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        TPCRequest.Reject(None)
    assert "tpc_key should be defined" in str(excinfo.value)

def test_apply(tpcr_all, tpcr_forwarded, tpcr_in_3pc, tpcr_ordered):
    def check(tpcr):
        tpcr.on_apply()
        assert tpcr.txn_state() == TxnState.Applied
        assert not tpcr.is_shallow()
        assert tpcr.is_applied()
        assert not tpcr.is_committed()

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded, tpcr_in_3pc, tpcr_ordered),
        check)

def test_commit(tpcr_all, tpcr_ordered_applied):
    def check(tpcr):
        tpcr.on_commit()
        assert tpcr.txn_state() == TxnState.Committed
        assert not tpcr.is_shallow()
        assert not tpcr.is_applied()
        assert tpcr.is_committed()

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
        assert tpcr.txn_state() == TxnState.Shallow
        assert tpcr.is_shallow()
        assert not tpcr.is_applied()
        assert not tpcr.is_committed()

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
        tpc_key = (0, 1)
        txn_state = tpcr.txn_state()
        tpcr.on_accept(tpc_key)
        assert tpcr.tpc_key == tpc_key
        assert tpcr.state() == TPCReqState.In3PC
        assert tpcr.txn_state() == txn_state

    check_statefuls(
        tpcr_all,
        (tpcr_forwarded,
         tpcr_forwarded_applied),
        check)

def test_reject(tpcr_all, tpcr_forwarded):
    def check(tpcr):
        tpc_key = (0, 1)
        txn_state = tpcr.txn_state()
        tpcr.on_reject(tpc_key)
        assert tpcr.tpc_key == tpc_key
        assert tpcr.state() == TPCReqState.Rejected
        assert tpcr.txn_state() == txn_state
        assert tpcr.is_rejected()

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
        assert tpcr.is_cleaned()

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
        tpc_key = tpcr.tpc_key
        states = list(tpcr.states)
        tpcr.on_reset()
        assert tpcr.state() == TPCReqState.Forwarded
        assert tpcr.is_reset()
        assert tpcr.tpc_key is None
        assert tpcr.states == [TPCReqState.Forwarded]
        assert tpcr.old_rounds == OrderedDict({tpc_key: tuple(states + [TPCReqState.Forwarded])})

        for ev in (tpcr.on_accept, tpcr.on_reject):
            with pytest.raises(ValueError) as excinfo:
                ev(tpc_key)
            assert ("TPC key {} was already used in previous rounds"
                    .format(tpc_key)) in str(excinfo.value)

    check_statefuls(
        tpcr_all,
        (tpcr_in_3pc,
         tpcr_ordered,
         tpcr_rejected,
         tpcr_cleaned_rejected,
         tpcr_cleaned_not_applied),
        check)
