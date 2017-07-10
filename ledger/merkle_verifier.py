import logging
from binascii import hexlify
from typing import Sequence, List

from ledger import error
from ledger.tree_hasher import TreeHasher
from ledger.util import STH


class MerkleVerifier(object):
    """A utility class for doing Merkle path computations."""

    def __init__(self, hasher=TreeHasher()):
        self.hasher = hasher

    def __repr__(self):
        return "%r(hasher: %r)" % (self.__class__.__name__, self.hasher)

    def __str__(self):
        return "%s(hasher: %s)" % (self.__class__.__name__, self.hasher)

    @error.returns_true_or_raises
    def verify_tree_consistency(self, old_tree_size: int, new_tree_size: int,
                                old_root: bytes, new_root: bytes,
                                proof: Sequence[bytes]):
        """Verify the consistency between two root hashes.

        old_tree_size must be <= new_tree_size.

        Args:
            old_tree_size: size of the older tree.
            new_tree_size: size of the newer_tree.
            old_root: the root hash of the older tree.
            new_root: the root hash of the newer tree.
            proof: the consistency proof.

        Returns:
            True. The return value is enforced by a decorator and need not be
                checked by the caller.

        Raises:
            ConsistencyError: the proof indicates an inconsistency
                (this is usually really serious!).
            ProofError: the proof is invalid.
            ValueError: supplied tree sizes are invalid.
        """
        old_size = old_tree_size
        new_size = new_tree_size

        if old_size < 0 or new_size < 0:
            raise ValueError("Negative tree size")

        if old_size > new_size:
            raise ValueError("Older tree has bigger size (%d vs %d), did "
                             "you supply inputs in the wrong order?" %
                             (old_size, new_size))

        if old_size == new_size:
            if old_root == new_root:
                if proof:
                    logging.warning("Trees are identical, ignoring proof")
                return True
            else:
                raise error.ConsistencyError("Inconsistency: different root "
                                             "hashes for the same tree size")

        if old_size == 0:
            if proof:
                # A consistency proof with an empty tree is an empty proof.
                # Anything is consistent with an empty tree, so ignore whatever
                # bogus proof was supplied. Note we do not verify here that the
                # root hash is a valid hash for an empty tree.
                logging.warning("Ignoring non-empty consistency proof for "
                                "empty tree.")
            return True

        # Now 0 < old_size < new_size
        # A consistency proof is essentially an audit proof for the node with
        # index old_size - 1 in the newer tree. The sole difference is that
        # the path is already hashed together into a single hash up until the
        # first audit node that occurs in the newer tree only.
        node = old_size - 1
        last_node = new_size - 1

        # While we are the right child, everything is in both trees, so move one
        # level up.
        while node % 2:
            node //= 2
            last_node //= 2

        p = iter(proof)
        try:
            if node:
                # Compute the two root hashes in parallel.
                new_hash = old_hash = next(p)
            else:
                # The old tree was balanced (2**k nodes), so we already have
                # the first root hash.
                new_hash = old_hash = old_root

            while node:
                if node % 2:
                    # node is a right child: left sibling exists in both trees.
                    next_node = next(p)
                    old_hash = self.hasher.hash_children(next_node, old_hash)
                    new_hash = self.hasher.hash_children(next_node, new_hash)
                elif node < last_node:
                    # node is a left child: right sibling only exists in the
                    # newer tree.
                    new_hash = self.hasher.hash_children(new_hash, next(p))
                # else node == last_node: node is a left child with no sibling
                # in either tree.
                node //= 2
                last_node //= 2

            # Now old_hash is the hash of the first subtree. If the two trees
            # have different height, continue the path until the new root.
            while last_node:
                n = next(p)
                new_hash = self.hasher.hash_children(new_hash, n)
                last_node //= 2

            # If the second hash does not match, the proof is invalid for the
            # given pair. If, on the other hand, the newer hash matches but the
            # older one doesn't, then the proof (together with the signatures
            # on the hashes) is proof of inconsistency.
            # Continue to find out.
            if new_hash != new_root:
                raise error.ProofError("Bad Merkle proof: second root hash "
                                       "does not match. Expected hash: %s "
                                       ", computed hash: %s" %
                                       (hexlify(new_root).strip(),
                                        hexlify(new_hash).strip()))
            elif old_hash != old_root:
                raise error.ConsistencyError("Inconsistency: first root hash "
                                             "does not match. Expected hash: "
                                             "%s, computed hash: %s" %
                                             (hexlify(old_root).strip(),
                                              hexlify(old_hash).strip())
                                             )

        except StopIteration:
            raise error.ProofError("Merkle proof is too short")

        # We've already verified consistency, so accept the proof even if
        # there's garbage left over (but log a warning).
        try:
            next(p)
        except StopIteration:
            pass
        else:
            logging.warning("Proof has extra nodes")
        return True

    def _calculate_root_hash_from_audit_path(self, leaf_hash: bytes,
                                             node_index: int,
                                             audit_path: List[bytes],
                                             tree_size: int):
        calculated_hash = leaf_hash
        last_node = tree_size - 1
        while last_node > 0:
            if not audit_path:
                raise error.ProofError('Proof too short: left with node index '
                                       '%d' % node_index)
            if node_index % 2:
                audit_hash = audit_path.pop(0)
                calculated_hash = self.hasher.hash_children(
                    audit_hash, calculated_hash)
            elif node_index < last_node:
                audit_hash = audit_path.pop(0)
                calculated_hash = self.hasher.hash_children(
                    calculated_hash, audit_hash)
            # node_index == last_node and node_index is even: A sibling does
            # not exist. Go further up the tree until node_index is odd so
            # calculated_hash will be used as the right-hand operand.
            node_index //= 2
            last_node //= 2
        if audit_path:
            raise error.ProofError('Proof too long: Left with %d hashes.' %
                                   len(audit_path))
        return calculated_hash

    @classmethod
    def audit_path_length(cls, index: int, tree_size: int):
        length = 0
        last_node = tree_size - 1
        while last_node > 0:
            if index % 2 or index < last_node:
                length += 1
            index //= 2
            last_node //= 2

        return length

    @error.returns_true_or_raises
    def verify_leaf_hash_inclusion(self, leaf_hash: bytes, leaf_index: int,
                                   proof: List[bytes], sth: STH):
        """Verify a Merkle Audit Path.

        See section 2.1.1 of RFC6962 for the exact path description.

        Args:
            leaf_hash: The hash of the leaf for which the proof was provided.
            leaf_index: Index of the leaf in the tree.
            proof: A list of SHA-256 hashes representing the  Merkle audit path.
            sth: STH with the same tree size as the one used to fetch the proof.
            The sha256_root_hash from this STH will be compared against the
            root hash produced from the proof.

        Returns:
            True. The return value is enforced by a decorator and need not be
                checked by the caller.

        Raises:
            ProofError: the proof is invalid.
        """
        leaf_index = int(leaf_index)
        tree_size = int(sth.tree_size)
        #TODO(eranm): Verify signature over STH
        if tree_size <= leaf_index:
            raise ValueError("Provided STH is for a tree that is smaller "
                             "than the leaf index. Tree size: %d Leaf "
                             "index: %d" % (tree_size, leaf_index))
        if tree_size < 0 or leaf_index < 0:
            raise ValueError("Negative tree size or leaf index: "
                                   "Tree size: %d Leaf index: %d" %
                                   (tree_size, leaf_index))
        calculated_root_hash = self._calculate_root_hash_from_audit_path(
                leaf_hash, leaf_index, proof[:], tree_size)
        if calculated_root_hash == sth.sha256_root_hash:
            return True

        raise error.ProofError("Constructed root hash differs from provided "
                               "root hash. Constructed: %s Expected: %s" %
                               (hexlify(calculated_root_hash).strip(),
                                hexlify(sth.sha256_root_hash).strip()))

    @error.returns_true_or_raises
    def verify_leaf_inclusion(self, leaf: bytes, leaf_index: int,
                                   proof: List[bytes], sth: STH):
        """Verify a Merkle Audit Path.

        See section 2.1.1 of RFC6962 for the exact path description.

        Args:
            leaf: The leaf for which the proof was provided.
            leaf_index: Index of the leaf in the tree.
            proof: A list of SHA-256 hashes representing the  Merkle audit path.
            sth: STH with the same tree size as the one used to fetch the proof.
            The sha256_root_hash from this STH will be compared against the
            root hash produced from the proof.

        Returns:
            True. The return value is enforced by a decorator and need not be
                checked by the caller.

        Raises:
            ProofError: the proof is invalid.
        """
        leaf_hash = self.hasher.hash_leaf(leaf)
        return self.verify_leaf_hash_inclusion(leaf_hash, leaf_index, proof,
                                               sth)
