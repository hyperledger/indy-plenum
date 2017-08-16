import hashlib

from ledger.util import count_bits_set


class TreeHasher(object):
    """Merkle hasher with domain separation for leaves and nodes."""

    def __init__(self, hashfunc=hashlib.sha256):
        self.hashfunc = hashfunc

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.hashfunc)

    def __str__(self):
        return repr(self)

    def hash_empty(self):
        hasher = self.hashfunc()
        return hasher.digest()

    def hash_leaf(self, data):
        hasher = self.hashfunc()
        hasher.update(b"\x00" + data)
        return hasher.digest()

    def hash_children(self, left, right):
        hasher = self.hashfunc()
        hasher.update(b"\x01" + left + right)
        return hasher.digest()

    def _hash_full(self, leaves, l_idx, r_idx):
        """Hash the leaves between (l_idx, r_idx) as a valid entire tree.

        Note that this is only valid for certain combinations of indexes,
        depending on where the leaves are meant to be located in a parent tree.

        Returns:
            (root_hash, hashes): where root_hash is that of the entire tree,
            and hashes are that of the full (i.e. size 2^k) subtrees that form
            the entire tree, sorted in descending order of size.
        """
        width = r_idx - l_idx
        if width < 0 or l_idx < 0 or r_idx > len(leaves):
            raise IndexError("%s,%s not a valid range over [0,%s]" % (
                l_idx, r_idx, len(leaves)))
        elif width == 0:
            return self.hash_empty(), ()
        elif width == 1:
            leaf_hash = self.hash_leaf(leaves[l_idx])
            return leaf_hash, (leaf_hash,)
        else:
            # next smallest power of 2
            split_width = 2**((width - 1).bit_length() - 1)
            assert split_width < width <= 2 * split_width
            l_root, l_hashes = self._hash_full(
                leaves, l_idx, l_idx + split_width)
            assert len(l_hashes) == 1  # left tree always full
            r_root, r_hashes = self._hash_full(
                leaves, l_idx + split_width, r_idx)
            root_hash = self.hash_children(l_root, r_root)
            return (root_hash, (root_hash,) if split_width * 2 == width else
                    l_hashes + r_hashes)

    def hash_full_tree(self, leaves):
        """Hash a set of leaves representing a valid full tree."""
        root_hash, hashes = self._hash_full(leaves, 0, len(leaves))
        assert len(hashes) == count_bits_set(len(leaves))
        assert (self._hash_fold(hashes) == root_hash if hashes else
                root_hash == self.hash_empty())
        return root_hash

    def _hash_fold(self, hashes):
        rev_hashes = iter(hashes[::-1])
        accum = next(rev_hashes)
        for cur in rev_hashes:
            accum = self.hash_children(cur, accum)
        return accum
