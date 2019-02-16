from stp_core.common.log import getlogger
from collections import deque
from common.exceptions import PlenumValueError, LogicError

logger = getlogger()


class LedgerUncommittedTracker:

    def __init__(self, last_committed_hash, ledger_size):
        self.un_committed = deque()
        self.set_last_committed(last_committed_hash, ledger_size)

    def apply_batch(self, state_root, ledger_size):
        """

        :param state_root: uncommitted state root
        :param ledger_size: ledger size, after committing (future ledger size)
        :return:
        """

        if not state_root:
            raise PlenumValueError('state_root',
                                   state_root,
                                   "No state root given")

        if ledger_size < 0:
            raise PlenumValueError('ledger_size',
                                   ledger_size,
                                   "Incorrect size of ledger")

        self.un_committed.append((state_root, ledger_size))

    def commit_batch(self):
        """

        :param state_root: committed state root
        :param ledger_size: committed ledger size
        :return: tuple of next committed state and count of committed transactions
        """
        if len(self.un_committed) == 0:
            raise PlenumValueError("un_committed",
                                   self.un_committed,
                                   "commit_batch was called, but there is no tracked uncommitted states")
        uncommitted_hash, uncommitted_size = self.un_committed.popleft()
        self.last_committed = (uncommitted_hash, uncommitted_size)

    def reject_batch(self):
        """
        Return hash reverting for and calculate count of reverted txns
        :return: root_hash, for reverting to (needed in revertToHead method) and count of reverted txns
        """
        prev_size = 0
        if len(self.un_committed) == 0:
            raise LogicError("No items to return")
        if len(self.un_committed) > 0:
            _, prev_size = self.un_committed.pop()
        if len(self.un_committed) == 0:
            committed_hash, committed_size = self.last_committed
            return committed_hash, prev_size - committed_size
        else:
            lhash, lsize = self.un_committed[-1]
            return lhash, prev_size - lsize

    def set_last_committed(self, state_root, ledger_size):
        self.last_committed = (state_root, ledger_size)
