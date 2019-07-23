from _sha256 import sha256
from collections import OrderedDict, defaultdict
from enum import IntEnum, unique
from typing import List

from sortedcontainers import SortedListWithKey

from common.exceptions import LogicError
from plenum.common.messages.node_messages import PrePrepare, Checkpoint
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData


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


class ConsensusDataHelper:
    def __init__(self, consensus_data: ConsensusSharedData):
        self.consensus_data = consensus_data

    def preprepare_batch(self, pp: PrePrepare):
        """
        After pp had validated, it placed into _preprepared list
        """
        if pp in self.consensus_data.preprepared:
            raise LogicError('New pp cannot be stored in preprepared')
        if self.consensus_data.checkpoints and pp.ppSeqNo < self.consensus_data.last_checkpoint.seqNoEnd:
            raise LogicError('ppSeqNo cannot be lower than last checkpoint')
        self.consensus_data.preprepared.append(pp)

    def prepare_batch(self, pp: PrePrepare):
        """
        After prepared certificate for pp had collected,
        it removed from _preprepared and placed into _prepared list
        """
        self.consensus_data.prepared.append(pp)

    def clear_batch(self, pp: PrePrepare):
        """
        When 3pc batch processed, it removed from _prepared list
        """
        if pp in self.consensus_data.preprepared:
            self.consensus_data.preprepared.remove(pp)
        if pp in self.consensus_data.prepared:
            self.consensus_data.prepared.remove(pp)

    def clear_batch_till_seq_no(self, seq_no):
        self.consensus_data.preprepared = [pp for pp in self.consensus_data.preprepared if pp.ppSeqNo >= seq_no]
        self.consensus_data.prepared = [p for p in self.consensus_data.prepared if p.ppSeqNo >= seq_no]

    def clear_all_batches(self):
        """
        Clear up all preprepared and prepared
        """
        self.consensus_data.prepared.clear()
        self.consensus_data.preprepared.clear()

    def add_checkpoint(self, checkpoint: Checkpoint):
        self.consensus_data.checkpoints.add(checkpoint)

    def reset_checkpoints(self):
        # That function most probably redundant in PBFT approach,
        # because according to paper, checkpoints cleared only when next stabilized.
        # Avoid using it while implement other services.
        self.consensus_data.checkpoints.clear()
        self.consensus_data.stable_checkpoint = 0

    def set_stable_checkpoint(self, end_seq_no):
        if not list(self.consensus_data.checkpoints.irange_key(end_seq_no, end_seq_no)):
            raise LogicError('Stable checkpoint must be in checkpoints')
        self.consensus_data.stable_checkpoint = end_seq_no

        self.consensus_data.checkpoints = \
            SortedListWithKey([c for c in self.consensus_data.checkpoints if c.seqNoEnd >= end_seq_no],
                              key=lambda checkpoint: checkpoint.seqNoEnd)
        self.clear_batch_till_seq_no(end_seq_no)


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