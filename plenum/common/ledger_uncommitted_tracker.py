from stp_core.common.log import getlogger
from collections import deque
from common.exceptions import PlenumValueError, LogicError

logger = getlogger()


class LedgerUncommittedTracker:

    def __init__(self):
        self.un_committed = deque()
        self.last_committed = None

    def apply_batch(self, state_root, ledger_size):

        if not state_root:
            raise PlenumValueError('state_root',
                                   state_root,
                                   "No state root given")

        if ledger_size <= 0:
            raise PlenumValueError('ledger_size',
                                   ledger_size,
                                   "Incorrect size of ledger")

        self.un_committed.append((state_root, ledger_size))

    def commit_batch(self):
        if len(self.un_committed) == 1:
            self.last_committed = None
        return self.un_committed.popleft()

    def reject_batch(self):
        prev_size = 0
        if len(self.un_committed) == 0 and self.last_committed is None:
            raise LogicError("No items to return")
        if len(self.un_committed) > 0:
            _, prev_size = self.un_committed.pop()
        if len(self.un_committed) == 0:
            committed_hash, committed_size = self.last_committed
            self.last_committed = None
            return committed_hash, prev_size - committed_size
        else:
            lhash, lsize = self.un_committed[-1]
            return lhash, prev_size - lsize
