from abc import abstractmethod, ABCMeta
from typing import Tuple
from collections import OrderedDict
from enum import unique, IntEnum

from stp_core.common.log import getlogger
from plenum.server.stateful import Stateful, TransitionError, StatefulEvent

logger = getlogger()


@unique
class TxnState(IntEnum):
    Shallow = 0     # initial state: neither applied nor committed
    Applied = 1     # applied but not committed
    Committed = 2   # committed (and not more applied)


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
    def __init__(self, tpc_key: Tuple[int, int]):
        if tpc_key is None:
            raise ValueError("tpc_key should be defined")
        self.tpc_key = tpc_key

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
    def __init__(self, rbft_request, inst_id: int):

        self.rbft_request = rbft_request
        self.inst_id = inst_id
        self.tpc_key = None
        self.old_rounds = OrderedDict()

        # TODO what about no state change transitions (e.g. Forwarded -> Forwarded)

        self._txn = Stateful(
            initial_state=TxnState.Shallow,
            transitions={
                TxnState.Shallow: TxnState.Applied,
                TxnState.Applied: self._is_applicable,
                TxnState.Committed: self._is_committable
            },
            name=("{}:{} request {} TxnState"
                  .format(self.rbft_request.node_name,
                          self.inst_id, repr(self.key)))
        )

        Stateful.__init__(
            self,
            initial_state=TPCReqState.Forwarded,
            transitions={
                TPCReqState.Forwarded: self._is_resettable,
                TPCReqState.In3PC: TPCReqState.Forwarded,
                TPCReqState.Ordered: TPCReqState.In3PC,
                TPCReqState.Rejected: self._is_rejectable,
                TPCReqState.Cancelled: self._is_cancellable,
                TPCReqState.Cleaned: self._is_cleanable
            }
        )

    def __repr__(self):
        return "{}:{} {}, {}, request: {}, tpc_key: {!r}, old_rounds: {!r}".format(
            self.rbft_request.node_name, self.inst_id,
            Stateful.__repr__(self), repr(self._txn),
            repr(self.key), self.tpc_key, self.old_rounds)

    # rules for transaction state
    def _is_applicable(self):
        return (self.is_shallow() and
                self.state() in (
                    TPCReqState.Forwarded,
                    TPCReqState.In3PC,
                    TPCReqState.Ordered))

    def _is_committable(self):
        return self.is_applied() and (self.state() == TPCReqState.Ordered)

    # rules for 3PC state
    def _is_resettable(self):
        # catch-up can cause that
        return (self.is_shallow() and
                self.state() != TPCReqState.Forwarded and
                not self.was_state(TPCReqState.Cancelled))

    def _is_rejectable(self):
        return self.is_shallow() and (self.state() == TPCReqState.Forwarded)

    def _is_cancellable(self):
        return (self.is_shallow() and
                self.state() in (
                    TPCReqState.Forwarded,
                    TPCReqState.In3PC,
                    TPCReqState.Ordered))

    def _is_cleanable(self):
        _state = self.state()
        if _state == TPCReqState.Cleaned:
            return False
        if _state in (TPCReqState.Rejected, TPCReqState.Cancelled):
            return True
        elif self.is_committed():
            return True
        elif self.is_shallow():
            return _state in (TPCReqState.Forwarded,
                              TPCReqState.In3PC,
                              TPCReqState.Ordered)

    # non-public methods
    def _set_txn_state(self, state: TxnState, dry: bool=False):
        try:
            self._txn.set_state(state, dry)
        except TransitionError as ex:
            ex.stateful = self
            raise ex

    # API
    @property
    def key(self):
        return self.rbft_request.key

    def txn_state(self):
        return self._txn.state()

    def is_shallow(self):
        return self._txn.state() == TxnState.Shallow

    def is_applied(self):
        return self._txn.state() == TxnState.Applied

    def is_committed(self):
        return self._txn.state() == TxnState.Committed

    def is_reset(self):
        """Returns True if TPCRequest has been reset but not started yet"""
        return self.old_rounds and (self.state() == TPCReqState.Forwarded)

    def is_rejected(self):
        # TODO as of now Rejected could be reset to Forwarded
        # and won't be as rejected in next 3CP round
        return self.was_state(TPCReqState.Rejected)

    def is_cleaned(self):
        return self.state() == TPCReqState.Cleaned

    # EVENTS processing
    def _on(self, ev, dry=False):
        if type(ev) == TPCReqApply:
            self._set_txn_state(TxnState.Applied, dry)
        elif type(ev) == TPCReqCommit:
            self._set_txn_state(TxnState.Committed, dry)
        elif type(ev) == TPCReqRevert:
            self._set_txn_state(TxnState.Shallow, dry)
        elif isinstance(ev, TPCReqPP):
            if ev.tpc_key in self.old_rounds:
                raise ValueError(
                    "TPC key {} was already used in previous rounds"
                    .format(ev.tpc_key))
            self.set_state(ev.new_state(), dry)
            if not dry:
                self.tpc_key = ev.tpc_key
        elif type(ev) == TPCReqOrder:
            self.set_state(TPCReqState.Ordered, dry)
        elif type(ev) == TPCReqCancel:
            self.set_state(TPCReqState.Cancelled, dry)
        elif type(ev) == TPCReqClean:
            self.set_state(TPCReqState.Cleaned, dry)
        elif type(ev) == TPCReqReset:
            self.set_state(TPCReqState.Forwarded, dry)
            if not dry:
                # transition rules guarantees that tpc_key is not None here
                assert self.tpc_key is not None
                self.old_rounds[self.tpc_key] = tuple(self.states)
                self.tpc_key = None
                self.states[:] = [TPCReqState.Forwarded]
        else:
            logger.warning("{!r} unexpected event type: {}".format(self, type(ev)))
