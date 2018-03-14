from abc import abstractmethod, ABCMeta
from typing import Tuple
from collections import OrderedDict
from enum import unique, IntEnum

from stp_core.common.log import getlogger
from plenum.server.stateful import Stateful, TransitionError, StatefulEvent

logger = getlogger()


@unique
class TransactionState(IntEnum):
    NotApplied = 0  # initial state
    Applied = 1     # applied but not committed
    Committed = 2   # committed


@unique
class TPCReqState(IntEnum):
    Forwarded = 0   # was forwarded to replica, waiting for 3PC routine
    In3PC = 1       # was added to (received in) some PrePrepare
    Ordered = 2     # was ordered
    Rejected = 3    # was rejected during 3PC batch creation
    Cancelled = 4   # was cancelled, no further activity except cleaning
    Cleaned = 5     # was cleaned (no more referenced)


class TPCReqEvent(StatefulEvent):
    """ Base class for events expected by TPCRequest """
    pass


class TPCReqApply(TPCReqEvent):
    pass


class TPCReqCommit(TPCReqEvent):
    pass


class TPCReqRevert(TPCReqEvent):
    pass


# received or sent inside some PP
class TPCReqPP(TPCReqEvent, metaclass=ABCMeta):
    def __init__(self, tpcKey: Tuple[int, int]):
        if tpcKey is None:
            raise ValueError("tpcKey should be defined")
        self.tpcKey = tpcKey

    @abstractmethod
    def new_state(self):
        pass


class TPCReqAccept(TPCReqPP):
    def new_state(self):
        return TPCReqState.In3PC


class TPCReqReject(TPCReqPP):
    def new_state(self):
        return TPCReqState.Rejected


class TPCReqOrder(TPCReqEvent):
    pass


class TPCReqCancel(TPCReqEvent):
    pass


class TPCReqClean(TPCReqEvent):
    pass


class TPCReqReset(TPCReqEvent):
    pass


class TPCRequest(Stateful):
    """
    3PC request
    """

    # events
    Apply = TPCReqApply
    Commit = TPCReqCommit
    Revert = TPCReqRevert
    Accept = TPCReqAccept
    Reject = TPCReqReject
    Order = TPCReqOrder
    Cancel = TPCReqCancel
    Clean = TPCReqClean
    Reset = TPCReqReset

    # TODO do we need RBFTRequest instance here
    def __init__(self, rbftRequest, instId: int):

        self.rbftRequest = rbftRequest
        self.instId = instId
        self.tpcKey = None
        self.old_rounds = OrderedDict()

        # TODO what about no state change transitions (e.g. Forwarded -> Forwarded)

        self.txn_state = Stateful(
            initialState=TransactionState.NotApplied,
            transitions={
                TransactionState.NotApplied: TransactionState.Applied,
                TransactionState.Applied: self._isApplicable,
                TransactionState.Committed: self._isCommittable
            },
            name='TxnState'
        )

        Stateful.__init__(
            self,
            initialState=TPCReqState.Forwarded,
            transitions={
                TPCReqState.Forwarded: self._isResettable,
                TPCReqState.In3PC: TPCReqState.Forwarded,
                TPCReqState.Ordered: TPCReqState.In3PC,
                TPCReqState.Rejected: self._isRejectable,
                TPCReqState.Cancelled: self._isCancellable,
                TPCReqState.Cleaned: self._isCleanable
            }
        )

    def __repr__(self):
        return "{}:{} {}, {}, request: {}, tpcKey: {!r}, old_rounds: {!r}".format(
            self.rbftRequest.nodeName, self.instId,
            Stateful.__repr__(self), repr(self.txn_state),
            repr(self.key), self.tpcKey, self.old_rounds)

    # rules for transaction state
    def _isApplicable(self):
        return ((self.txn_state.state() == TransactionState.NotApplied) and
                self.state() in (
                    TPCReqState.Forwarded,
                    TPCReqState.In3PC,
                    TPCReqState.Ordered))

    def _isCommittable(self):
        return ((self.txn_state.state() == TransactionState.Applied) and
                (self.state() == TPCReqState.Ordered))

    # rules for 3PC state
    def _isResettable(self):
        # catch-up can cause that
        return not (self.state() == TPCReqState.Forwarded or
                    self.isApplied() or
                    self.wasState(TPCReqState.Cancelled))

    def _isRejectable(self):
        return (self.state() == TPCReqState.Forwarded) and not self.isApplied()

    def _isCancellable(self):
        # TODO what about ordered but not committed yet
        return (not self.isApplied() and
                self.state() in (
                    TPCReqState.Forwarded,
                    TPCReqState.In3PC,
                    TPCReqState.Ordered))

    def _isCleanable(self):
        _state = self.state()
        if _state == TPCReqState.Cleaned:
            return False
        if _state in (TPCReqState.Rejected, TPCReqState.Cancelled):
            return True
        elif self.isCommitted():
            return True
        elif not self.isApplied():
            return _state in (TPCReqState.Forwarded,
                              TPCReqState.In3PC,
                              TPCReqState.Ordered)

    # non-public methods
    def _setTxnState(self, state: TransactionState, dry: bool=False):
        try:
            self.txn_state.setState(state, dry)
        except TransitionError as ex:
            ex.stateful = self
            raise ex

    # API
    @property
    def key(self):
        return self.rbftRequest.key

    def txnState(self):
        return self.txn_state.state()

    def isApplied(self):
        return (self.txn_state.state() in
                (TransactionState.Applied, TransactionState.Committed))

    def isCommitted(self):
        return self.txn_state.state() == TransactionState.Committed

    def isReset(self):
        """Returns True if TPCRequest has been reset but not started yet"""
        return self.old_rounds and (self.state() == TPCReqState.Forwarded)

    def isRejected(self):
        # TODO as of now Rejected could be reset to Forwarded
        # and won't be as rejected in next 3CP round
        return self.wasState(TPCReqState.Rejected)

    def isCleaned(self):
        return self.state() == TPCReqState.Cleaned

    # EVENTS processing
    def _on(self, ev, dry=False):
        if type(ev) == TPCReqApply:
            self._setTxnState(TransactionState.Applied, dry)
        elif type(ev) == TPCReqCommit:
            self._setTxnState(TransactionState.Committed, dry)
        elif type(ev) == TPCReqRevert:
            self._setTxnState(TransactionState.NotApplied, dry)
        elif isinstance(ev, TPCReqPP):
            if ev.tpcKey in self.old_rounds:
                raise ValueError(
                    "TPC key {} was already used in previous rounds"
                    .format(ev.tpcKey))
            self.setState(ev.new_state(), dry)
            if not dry:
                self.tpcKey = ev.tpcKey
        elif type(ev) == TPCReqOrder:
            self.setState(TPCReqState.Ordered, dry)
        elif type(ev) == TPCReqCancel:
            self.setState(TPCReqState.Cancelled, dry)
        elif type(ev) == TPCReqClean:
            self.setState(TPCReqState.Cleaned, dry)
        elif type(ev) == TPCReqReset:
            self.setState(TPCReqState.Forwarded, dry)
            if not dry:
                # transition rules guarantees that tpcKey is not None here
                assert self.tpcKey is not None
                self.old_rounds[self.tpcKey] = tuple(self.states)
                self.tpcKey = None
                self.states[:] = [TPCReqState.Forwarded]
        else:
            logger.warning("{!r} unexpected event type: {}".format(self, type(ev)))
