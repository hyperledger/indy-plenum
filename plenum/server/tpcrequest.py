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
    """ Base class for event expected by TPCRequest """
    pass


class TPCRequest(Stateful):
    """
    3PC request
    """
    # TODO do we need RBFTRequest instance here
    def __init__(self, rbftRequest, instId: int):

        self.rbftRequest = rbftRequest
        self.instId = instId
        self.tpcKey = None
        self.old_rounds = OrderedDict()

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
            },
            stateful_event_class=TPCReqEvent
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
    def _isRejectable(self):
        return (self.state() == TPCReqState.Forwarded) and not self.isApplied()

    def _isResettable(self):
        # catch-up can cause that
        # TODO what if cleaned rejected requests are started processing
        # in new 3PC round, thus Cleaned->Forwarded is possible
        return ((self.state() == TPCReqState.Rejected) or
                ((self.state() == TPCReqState.In3PC) and not self.isApplied()))

    def _isCancellable(self):
        # TODO what about ordered but not committed yet
        return (not (self.isApplied() or self.isCommitted()) and
                self.state() in (
                    TPCReqState.Forwarded,
                    TPCReqState.In3PC,
                    TPCReqState.Ordered))

    def _isCleanable(self):
        _state = self.state()
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
        return self.txn_state.state() == TransactionState.Applied

    def isCommitted(self):
        return self.txn_state.state() == TransactionState.Committed

    def isRejected(self):
        # TODO as of now Rejected could be reset to Forwarded
        # and won't be as rejected in next 3CP round
        return self.wasState(TPCReqState.Rejected)

    def isCleaned(self):
        return self.state() == TPCReqState.Cleaned

    # EVENTS
    class Apply(TPCReqEvent):
        def react(self, tpcReq, dry: bool=False):
            tpcReq._setTxnState(TransactionState.Applied, dry)

    class Commit(TPCReqEvent):
        def react(self, tpcReq, dry: bool=False):
            tpcReq._setTxnState(TransactionState.Committed, dry)

    class Revert(TPCReqEvent):
        def react(self, tpcReq, dry: bool=False):
            tpcReq._setTxnState(TransactionState.NotApplied, dry)

    # received or sent inside some PP
    class PP(TPCReqEvent, metaclass=ABCMeta):
        def __init__(self, tpcKey: Tuple[int, int]):
            self.tpcKey = tpcKey

        @abstractmethod
        def new_state(self):
            pass

        def react(self, tpcReq, dry: bool=False):
            tpcReq.setState(self.new_state(), dry)
            if not dry:
                tpcReq.tpcKey = self.tpcKey

    class Accept(PP):
        def new_state(self):
            return TPCReqState.In3PC

    class Reject(PP):
        def new_state(self):
            return TPCReqState.Rejected

    class Order(TPCReqEvent):
        def react(self, tpcReq, dry: bool=False):
            tpcReq.setState(TPCReqState.Ordered, dry)

    class Cancel(TPCReqEvent):
        def react(self, tpcReq, dry: bool=False):
            tpcReq.setState(TPCReqState.Cancelled, dry)

    class Clean(TPCReqEvent):
        def react(self, tpcReq, dry: bool=False):
            tpcReq.setState(TPCReqState.Cleaned, dry)

    class Reset(TPCReqEvent):
        def react(self, tpcReq, dry: bool=False):
            tpcReq.setState(TPCReqState.Forwarded, dry)
            if not dry:
                # transition rules guarantees that tpcKey is not None here
                assert tpcReq.tpcKey is not None
                tpcReq.old_rounds[tuple(tpcReq.tpcKey)] = list(tpcReq.states)
                tpcReq.tpcKey = None
                tpcReq.states[:] = [TPCReqState.Forwarded]
