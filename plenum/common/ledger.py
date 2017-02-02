from copy import copy
from typing import List, Tuple

from ledger.ledger import Ledger as _Ledger


class Ledger(_Ledger):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Merkle tree of containing transactions that have not yet been
        # committed but optimistically applied.
        self.uncommittedTxns = []
        self.uncommittedRootHash = None
        self.uncommittedTree = None

    def appendTxns(self, txns: List):
        self.uncommittedTree = self.treeWithAppliedTxns(txns,
                                                        self.uncommittedTree)
        self.uncommittedRootHash = self.uncommittedTree.root_hash
        self.uncommittedTxns.append(txns)

    def commitTxns(self, count: int) -> Tuple[int, int]:
        """
        The number of txns from the beginning of `uncommittedTxns` to commit
        :param count:
        :return: a tuple of 2 seqNos indicating the start and end of sequence
        numbers of the committed txns
        """
        committedSize = self.size
        for txn in self.uncommittedTxns[:count]:
            self.append(txn)
        self.discardTxns(count)
        return committedSize + 1, committedSize + count

    def appendCommittedTxns(self, txns: List):
        # Called while receiving committed txns from other nodes
        self.append(txns)

    def discardTxns(self, index: int):
        """
        The index in `uncommittedTxns` from which txns have to be discarded
        :param index:
        :return:
        """
        self.uncommittedTxns = self.uncommittedTxns[index:]
        if not self.uncommittedTxns:
            self.uncommittedTree = None
            self.uncommittedRootHash = None

    def treeWithAppliedTxns(self, txns: List, currentTree=None):
        """
        Return a copy of merkle tree after applying the txns
        :param txns:
        :return:
        """
        currentTree = currentTree or self.tree
        tempTree = copy(currentTree)
        for txn in txns:
            tempTree.append(self.serializeLeaf(txn))
        return tempTree
