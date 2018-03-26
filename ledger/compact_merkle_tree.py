import functools
from binascii import hexlify
from typing import List, Tuple, Sequence

import ledger.merkle_tree as merkle_tree
from ledger.hash_stores.hash_store import HashStore
from ledger.hash_stores.memory_hash_store import MemoryHashStore
from ledger.tree_hasher import TreeHasher
from ledger.util import ConsistencyVerificationFailed
from ledger.util import count_bits_set, lowest_bit_set


class CompactMerkleTree(merkle_tree.MerkleTree):
    """Compact representation of a Merkle Tree that permits only extension.

    Attributes:
        tree_size: Number of leaves in this tree.
        hashes: That of the full (i.e. size 2^k) subtrees that form this tree,
            sorted in descending order of size.
    """

    def __init__(self, hasher=TreeHasher(), tree_size=0, hashes=(),
                 hashStore=None):

        # These two queues should be written to two simple position-accessible
        # arrays (files, database tables, etc.)
        self.__hashStore = hashStore or MemoryHashStore()  # type: HashStore
        self.__hasher = hasher
        self._update(tree_size, hashes)

    @property
    def hashStore(self):
        return self.__hashStore

    def _update(self, tree_size: int, hashes: Sequence[bytes]):
        bits_set = count_bits_set(tree_size)
        num_hashes = len(hashes)
        if num_hashes != bits_set:
            msgfmt = "number of hashes != bits set in tree_size: %s vs %s"
            raise ValueError(msgfmt % (num_hashes, bits_set))
        self.__tree_size = tree_size
        self.__hashes = tuple(hashes)
        # height of the smallest subtree, or 0 if none exists (empty tree)
        self.__mintree_height = lowest_bit_set(tree_size)
        self.__root_hash = None

    def load(self, other: merkle_tree.MerkleTree):
        """Load this tree from a dumb data object for serialisation.

        The object must have attributes tree_size:int and hashes:list.
        """
        self._update(other.tree_size, other.hashes)

    def save(self, other: merkle_tree.MerkleTree):
        """Save this tree into a dumb data object for serialisation.

        The object must have attributes tree_size:int and hashes:list.
        """
        other.__tree_size = self.__tree_size
        other.__hashes = self.__hashes

    def __copy__(self):
        return self.__class__(self.__hasher, self.__tree_size, self.__hashes)

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__,
            self.__hasher, self.__tree_size, self.__hashes)

    def __len__(self):
        return self.__tree_size

    @property
    def tree_size(self) -> int:
        return self.__tree_size

    @property
    def hashes(self) -> Tuple[bytes]:
        return self.__hashes

    @property
    def root_hash(self):
        """Returns the root hash of this tree. (Only re-computed on change.)"""
        if self.__root_hash is None:
            self.__root_hash = (
                self.__hasher._hash_fold(self.__hashes)
                if self.__hashes else self.__hasher.hash_empty())
        return self.__root_hash

    @property
    def root_hash_hex(self):
        """Returns the root hash of this tree. (Only re-computed on change.)"""
        return hexlify(self.root_hash)

    def _push_subtree(self, leaves: List[bytes]):
        """Extend with a full subtree <= the current minimum subtree.

        The leaves must form a full subtree, i.e. of size 2^k for some k. If
        there is a minimum subtree (i.e. __mintree_height > 0), then the input
        subtree must be smaller or of equal size to the minimum subtree.

        If the subtree is smaller (or no such minimum exists, in an empty
        tree), we can simply append its hash to self.hashes, since this
        maintains the invariant property of being sorted in descending
        size order.

        If the subtree is of equal size, we are in a similar situation to an
        addition carry. We handle it by combining the two subtrees into a
        larger subtree (of size 2^(k+1)), then recursively trying to add
        this new subtree back into the tree.

        Any collection of leaves larger than the minimum subtree must undergo
        additional partition to conform with the structure of a merkle tree,
        which is a more complex operation, performed by extend().
        """
        size = len(leaves)
        if count_bits_set(size) != 1:
            raise ValueError("invalid subtree with size != 2^k: %s" % size)
        # in general we want the highest bit, but here it's also the lowest bit
        # so just reuse that code instead of writing a new highest_bit_set()
        subtree_h, mintree_h = lowest_bit_set(size), self.__mintree_height
        if mintree_h > 0 and subtree_h > mintree_h:
            raise ValueError("subtree %s > current smallest subtree %s" % (
                subtree_h, mintree_h))
        root_hash, hashes = self.__hasher._hash_full(leaves, 0, size)
        assert hashes == (root_hash,)

        if self.hashStore:
            for h in hashes:
                self.hashStore.writeLeaf(h)

        new_node_hashes = self.__push_subtree_hash(subtree_h, root_hash)

        nodes = [(self.tree_size, height, h) for h, height in new_node_hashes]
        if self.hashStore:
            for node in nodes:
                self.hashStore.writeNode(node)

    def __push_subtree_hash(self, subtree_h: int, sub_hash: bytes):
        size, mintree_h = 1 << (subtree_h - 1), self.__mintree_height
        if subtree_h < mintree_h or mintree_h == 0:
            self._update(self.tree_size + size, self.hashes + (sub_hash,))
            return []
        else:
            assert subtree_h == mintree_h
            # addition carry - rewind the tree and re-try with bigger subtree
            prev_hash = self.hashes[-1]
            self._update(self.tree_size - size, self.hashes[:-1])
            new_mintree_h = self.__mintree_height
            assert mintree_h < new_mintree_h or new_mintree_h == 0
            next_hash = self.__hasher.hash_children(prev_hash, sub_hash)

            return [(next_hash, subtree_h)] + self.__push_subtree_hash(
                subtree_h + 1, next_hash)

    def append(self, new_leaf: bytes) -> List[bytes]:
        """Append a new leaf onto the end of this tree and return the
        audit path"""
        auditPath = list(reversed(self.__hashes))
        self._push_subtree([new_leaf])
        return auditPath

    def extend(self, new_leaves: List[bytes]):
        """Extend this tree with new_leaves on the end.

        The algorithm works by using _push_subtree() as a primitive, calling
        it with the maximum number of allowed leaves until we can add the
        remaining leaves as a valid entire (non-full) subtree in one go.
        """
        size = len(new_leaves)
        final_size = self.tree_size + size
        idx = 0
        while True:
            # keep pushing subtrees until mintree_size > remaining
            max_h = self.__mintree_height
            max_size = 1 << (max_h - 1) if max_h > 0 else 0
            if max_h > 0 and size - idx >= max_size:
                self._push_subtree(new_leaves[idx:idx + max_size])
                idx += max_size
            else:
                break
        # fill in rest of tree in one go, now that we can
        if idx < size:
            root_hash, hashes = self.__hasher._hash_full(new_leaves, idx, size)
            self._update(final_size, self.hashes + hashes)
        assert self.tree_size == final_size

    def extended(self, new_leaves: List[bytes]):
        """Returns a new tree equal to this tree extended with new_leaves."""
        new_tree = self.__copy__()
        new_tree.extend(new_leaves)
        return new_tree

    def merkle_tree_hash_hex(self, start: int, end: int):
        mth = self.merkle_tree_hash(start, end)
        return hexlify(mth)

    @functools.lru_cache(maxsize=256)
    def merkle_tree_hash(self, start: int, end: int):
        if not end > start:
            raise ValueError("end must be greater than start")
        if (end - start) == 1:
            return self.hashStore.readLeaf(end)
        leafs, nodes = self.hashStore.getPath(end, start)
        leafHash = self.hashStore.readLeaf(end)
        hashes = [leafHash, ]
        for h in leafs:
            hashes.append(self.hashStore.readLeaf(h))
        for h in nodes:
            hashes.append(self.hashStore.readNode(h))
        foldedHash = self.__hasher._hash_fold(hashes[::-1])
        return foldedHash

    def consistency_proof(self, first: int, second: int):
        return [self.merkle_tree_hash(a, b) for a, b in
                self._subproof(first, 0, second, True)]

    def inclusion_proof(self, start, end):
        return [self.merkle_tree_hash(a, b)
                for a, b in self._path(start, 0, end)]

    def _subproof(self, m, start_n: int, end_n: int, b: int):
        n = end_n - start_n
        if m == n:
            if b:
                return []
            else:
                return [(start_n, end_n)]
        else:
            k = 1 << (len(bin(n - 1)) - 3)
            if m <= k:
                return self._subproof(m, start_n, start_n + k, b) + [
                    (start_n + k, end_n)]
            else:
                return self._subproof(m - k, start_n + k, end_n, False) + [
                    (start_n, start_n + k)]

    def _path(self, m, start_n: int, end_n: int):
        n = end_n - start_n
        if n == 1:
            return []
        else:
            # `k` is the largest power of 2 less than `n`
            k = 1 << (len(bin(n - 1)) - 3)
            if m < k:
                return self._path(m, start_n, start_n + k) + [
                    (start_n + k, end_n)]
            else:
                return self._path(m - k, start_n + k, end_n) + [
                    (start_n, start_n + k)]

    def get_tree_head(self, seq: int = None):
        if seq is None:
            seq = self.tree_size
        if seq > self.tree_size:
            raise IndexError
        return {
            'tree_size': seq,
            'sha256_root_hash': self.merkle_tree_hash(0, seq) if seq else None,
        }

    @property
    def leafCount(self) -> int:
        return self.hashStore.leafCount

    @property
    def nodeCount(self) -> int:
        return self.hashStore.nodeCount

    @staticmethod
    def get_expected_node_count(leaf_count):
        """
        The number of nodes is the number of full subtrees present
        """
        count = 0
        while leaf_count > 1:
            leaf_count //= 2
            count += leaf_count
        return count

    def verify_consistency(self, expected_leaf_count) -> bool:
        """
        Check that the tree has same leaf count as expected and the
        number of nodes are also as expected
        """
        if expected_leaf_count != self.leafCount:
            raise ConsistencyVerificationFailed()
        if self.get_expected_node_count(self.leafCount) != self.nodeCount:
            raise ConsistencyVerificationFailed()
        return True

    def reset(self):
        self.hashStore.reset()
        self._update(tree_size=0,
                     hashes=())
