from stp_core.common.log import getlogger
from collections import deque
from common.exceptions import PlenumValueError, LogicError

logger = getlogger()


class LedgerUncommittedTracker:

    def __init__(self):
        self.un_committed = deque()

    def apply_batch(self, state_root, ledger_size):
        if state_root is None:
            raise PlenumValueError
        if (ledger_size is None) or ledger_size <= 0:
            raise PlenumValueError

        self.un_committed.append((state_root, ledger_size))

    def commit_batch(self):
        return self.un_committed.popleft()

    def reject_batch(self):
        if len(self.un_committed) == 0:
            raise LogicError
        self.un_committed.pop()
