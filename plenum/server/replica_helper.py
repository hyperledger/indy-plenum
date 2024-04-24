from _sha256 import sha256
from collections import defaultdict, OrderedDict
from enum import IntEnum, unique
from typing import List

PP_CHECK_NOT_FROM_PRIMARY = 0
PP_CHECK_TO_PRIMARY = 1
PP_CHECK_DUPLICATE = 2
PP_CHECK_OLD = 3
PP_CHECK_REQUEST_NOT_FINALIZED = 4
PP_CHECK_NOT_NEXT = 5
PP_CHECK_WRONG_TIME = 6
PP_CHECK_INCORRECT_POOL_STATE_ROOT = 14

PP_APPLY_REJECT_WRONG = 7
PP_APPLY_WRONG_DIGEST = 8
PP_APPLY_WRONG_STATE = 9
PP_APPLY_ROOT_HASH_MISMATCH = 10
PP_APPLY_HOOK_ERROR = 11
PP_SUB_SEQ_NO_WRONG = 12
PP_NOT_FINAL = 13
PP_APPLY_AUDIT_HASH_MISMATCH = 15
PP_REQUEST_ALREADY_ORDERED = 16
PP_WRONG_PRIMARIES = 17


def generateName(nodeName: str, instId: int):
    """
    Create and return the name for a replica using its nodeName and
    instanceId.
     Ex: Alpha:1
    """

    if isinstance(nodeName, str):
        # Because sometimes it is bytes (why?)
        if ":" in nodeName:
            # Because in some cases (for requested messages) it
            # already has ':'. This should be fixed.
            return nodeName
    return "{}:{}".format(nodeName, instId)


@unique
class TPCStat(IntEnum):  # TPC => Three-Phase Commit
    ReqDigestRcvd = 0
    PrePrepareSent = 1
    PrePrepareRcvd = 2
    PrepareRcvd = 3
    PrepareSent = 4
    CommitRcvd = 5
    CommitSent = 6
    OrderSent = 7


class Stats:
    def __init__(self, keys):
        sort = sorted([k.value for k in keys])
        self.stats = OrderedDict((s, 0) for s in sort)

    def inc(self, key):
        """
        Increment the stat specified by key.
        """
        self.stats[key] += 1

    def get(self, key):
        return self.stats[key]

    def __repr__(self):
        return str({TPCStat(k).name: v for k, v in self.stats.items()})


class IntervalList:
    def __init__(self):
        self._intervals = []

    def __len__(self):
        return sum(i[1] - i[0] + 1 for i in self._intervals)

    def __eq__(self, other):
        if not isinstance(other, IntervalList):
            return False
        return self._intervals == other._intervals

    def __contains__(self, item):
        return any(i[0] <= item <= i[1] for i in self._intervals)

    def add(self, item):
        if len(self._intervals) == 0:
            self._intervals.append([item, item])
            return

        if item < self._intervals[0][0] - 1:
            self._intervals.insert(0, [item, item])
            return

        if item == self._intervals[0][0] - 1:
            self._intervals[0][0] -= 1
            return

        if self._intervals[0][0] <= item <= self._intervals[0][1]:
            return

        for prev, next in zip(self._intervals, self._intervals[1:]):
            if item == prev[1] + 1:
                prev[1] += 1
                if prev[1] == next[0] - 1:
                    prev[1] = next[1]
                    self._intervals.remove(next)
                return

            if prev[1] + 1 < item < next[0] - 1:
                idx = self._intervals.index(next)
                self._intervals.insert(idx, [item, item])
                return

            if item == next[0] - 1:
                next[0] -= 1
                return

            if next[0] <= item <= next[1]:
                return

        if item == self._intervals[-1][1] + 1:
            self._intervals[-1][1] += 1
            return

        self._intervals.append([item, item])


class OrderedTracker:
    def __init__(self):
        self._batches = defaultdict(IntervalList)

    def __len__(self):
        return sum(len(il) for il in self._batches.values())

    def __eq__(self, other):
        if not isinstance(other, OrderedTracker):
            return False
        return self._batches == other._batches

    def __contains__(self, item):
        view_no, pp_seq_no = item
        return pp_seq_no in self._batches[view_no]

    def add(self, view_no, pp_seq_no):
        self._batches[view_no].add(pp_seq_no)

    def clear_below_view(self, view_no):
        for v in list(self._batches.keys()):
            if v < view_no:
                del self._batches[v]


def replica_batch_digest(reqs: List):
    return sha256(b''.join([r.digest.encode() for r in reqs])).hexdigest()
