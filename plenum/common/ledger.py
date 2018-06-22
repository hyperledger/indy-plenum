from copy import copy
from typing import List, Tuple

from common.exceptions import PlenumValueError
from ledger.ledger import Ledger as _Ledger
from ledger.util import F
from plenum.common.txn_util import append_txn_metadata, get_seq_no
from stp_core.common.log import getlogger

logger = getlogger()


class Ledger(_Ledger):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Merkle tree of containing transactions that have not yet been
        # committed but optimistically applied.
        self.uncommittedTxns = []
        self.uncommittedRootHash = None
        self.uncommittedTree = None

    @property
    def uncommitted_size(self) -> int:
        return self.size + len(self.uncommittedTxns)

    def append_txns_metadata(self, txns: List, txn_time=None):
        if txn_time is not None:
            # All transactions have the same time since all these
            # transactions belong to the same 3PC batch
            for txn in txns:
                append_txn_metadata(txn, txn_time=txn_time)
        self._append_seq_no(txns, self.seqNo + len(self.uncommittedTxns))

    def appendTxns(self, txns: List):
        # These transactions are not yet committed so they do not go to
        # the ledger
        _no_seq_no_txns = [txn for txn in txns if get_seq_no(txn) is None]
        if _no_seq_no_txns:
            raise PlenumValueError(
                'txns', txns,
                ("all txns should have defined seq_no, undefined in {}"
                 .format(_no_seq_no_txns))
            )

        uncommittedSize = self.size + len(self.uncommittedTxns)
        self.uncommittedTree = self.treeWithAppliedTxns(txns,
                                                        self.uncommittedTree)
        self.uncommittedRootHash = self.uncommittedTree.root_hash
        self.uncommittedTxns.extend(txns)
        if txns:
            return (uncommittedSize + 1, uncommittedSize + len(txns)), txns
        else:
            return (uncommittedSize, uncommittedSize), txns

    def add(self, txn):
        if get_seq_no(txn) is None:
            self._append_seq_no([txn], self.seqNo)
        merkle_info = super().add(txn)
        # seqNo is part of the transaction itself, so no need to duplicate it here
        merkle_info.pop(F.seqNo.name, None)
        return merkle_info

    def _append_seq_no(self, txns, start_seq_no):
        # TODO: Fix name `start_seq_no`, it is misleading. The seq no start from `start_seq_no`+1
        seq_no = start_seq_no
        for txn in txns:
            seq_no += 1
            append_txn_metadata(txn, seq_no=seq_no)
        return txns

    def commitTxns(self, count: int) -> Tuple[Tuple[int, int], List]:
        """
        The number of txns from the beginning of `uncommittedTxns` to commit
        :param count:
        :return: a tuple of 2 seqNos indicating the start and end of sequence
        numbers of the committed txns
        """
        committedSize = self.size
        committedTxns = []
        for txn in self.uncommittedTxns[:count]:
            txn.update(self.append(txn))
            committedTxns.append(txn)
        self.uncommittedTxns = self.uncommittedTxns[count:]
        logger.debug('Committed {} txns, {} are uncommitted'.
                     format(len(committedTxns), len(self.uncommittedTxns)))
        if not self.uncommittedTxns:
            self.uncommittedTree = None
            self.uncommittedRootHash = None
        # Do not change `uncommittedTree` or `uncommittedRootHash`
        # if there are any `uncommittedTxns` since the ledger still has a
        # valid uncommittedTree and a valid root hash which are
        # different from the committed ones
        if committedTxns:
            return (committedSize + 1, committedSize + count), committedTxns
        else:
            return (committedSize, committedSize), committedTxns

    def discardTxns(self, count: int):
        """
        The number of txns in `uncommittedTxns` which have to be
        discarded
        :param count:
        :return:
        """
        # TODO: This can be optimised if multiple discards are combined
        # together since merkle root computation will be done only once.
        if count == 0:
            return
        old_hash = self.uncommittedRootHash
        self.uncommittedTxns = self.uncommittedTxns[:-count]
        if not self.uncommittedTxns:
            self.uncommittedTree = None
            self.uncommittedRootHash = None
        else:
            self.uncommittedTree = self.treeWithAppliedTxns(
                self.uncommittedTxns)
            self.uncommittedRootHash = self.uncommittedTree.root_hash
        logger.info('Discarding {} txns and root hash {} and new root hash '
                    'is {}. {} are still uncommitted'.
                    format(count, old_hash, self.uncommittedRootHash,
                           len(self.uncommittedTxns)))

    def treeWithAppliedTxns(self, txns: List, currentTree=None):
        """
        Return a copy of merkle tree after applying the txns
        :param txns:
        :return:
        """
        currentTree = currentTree or self.tree
        # Copying the tree is not a problem since its a Compact Merkle Tree
        # so the size of the tree would be 32*(lg n) bytes where n is the
        # number of leaves (no. of txns)
        tempTree = copy(currentTree)
        for txn in txns:
            s = self.serialize_for_tree(txn)
            tempTree.append(s)
        return tempTree

    def reset_uncommitted(self):
        self.uncommittedTxns = []
        self.uncommittedRootHash = None
        self.uncommittedTree = None
