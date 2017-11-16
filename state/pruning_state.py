from binascii import unhexlify
from typing import Optional

from state.db.persistent_db import PersistentDB
from state.state import State
from state.trie.pruning_trie import BLANK_ROOT, Trie, BLANK_NODE, \
    bin_to_nibbles
from state.util.fast_rlp import encode_optimized as rlp_encode, \
    decode_optimized as rlp_decode
from state.util.utils import to_string, isHex
from storage.kv_store import KeyValueStorage


class PruningState(State):
    """
    This class is used to store the
    committed root hash of the trie in the db.
    The committed root hash is only updated once a batch gets written to the
    ledger. It might happen that a few batches are in 3 phase commit and the
    node crashes. Now when the node restarts, it restores the db from the
    committed root hash and all entries for uncommitted batches will be
    ignored
    """

    # SOME KEY THAT DOES NOT COLLIDE WITH ANY STATE VARIABLE'S NAME
    rootHashKey = b'\x88\xc8\x88 \x9a\xa7\x89\x1b'

    def __init__(self, keyValueStorage: KeyValueStorage):
        self._kv = keyValueStorage
        if self.rootHashKey in self._kv:
            rootHash = bytes(self._kv.get(self.rootHashKey))
        else:
            rootHash = BLANK_ROOT
            self._kv.put(self.rootHashKey, BLANK_ROOT)
        self._trie = Trie(
            PersistentDB(self._kv),
            rootHash)

    @property
    def head(self):
        # The current head of the state, if the state is a merkle tree then
        # head is the root
        return self._trie.root_node

    @property
    def committedHead(self):
        # The committed head of the state, if the state is a merkle tree then
        # head is the root
        return self._hash_to_node(self.committedHeadHash)

    def _hash_to_node(self, node_hash):
        if node_hash == BLANK_ROOT:
            return BLANK_NODE
        return self._trie._decode_to_node(node_hash)

    def set(self, key: bytes, value: bytes):
        self._trie.update(key, rlp_encode([value]))

    def get(self, key: bytes, isCommitted: bool = True) -> Optional[bytes]:
        if not isCommitted:
            val = self._trie.get(key)
        else:
            val = self._trie._get(self.committedHead,
                                  bin_to_nibbles(to_string(key)))
        if val:
            return rlp_decode(val)[0]

    def get_for_root_hash(self, root_hash, key: bytes) -> Optional[bytes]:
        root = self._hash_to_node(root_hash)
        val = self._trie._get(root,
                              bin_to_nibbles(to_string(key)))
        if val:
            return rlp_decode(val)[0]

    def remove(self, key: bytes):
        self._trie.delete(key)

    def commit(self, rootHash=None, rootNode=None):
        if rootNode:
            rootHash = self._trie._encode_node(rootNode)
        elif rootHash and isHex(rootHash):
            if isinstance(rootHash, str):
                rootHash = rootHash.encode()
            rootHash = unhexlify(rootHash)
        elif rootHash:
            rootHash = rootHash
        else:
            rootHash = self.headHash
        self._kv.put(self.rootHashKey, rootHash)

    def revertToHead(self, headHash=None):
        head = self._hash_to_node(headHash)
        self._trie.replace_root_hash(self._trie.root_node, head)

    # Proofs are always generated over committed state
    def generate_state_proof(self, key: bytes, root=None, serialize=False):
        return self._trie.generate_state_proof(key, root, serialize)

    @staticmethod
    def verify_state_proof(root, key, value, proof_nodes, serialized=False):
        encoded_value = rlp_encode([value]) if value is not None else b''
        return Trie.verify_spv_proof(root, key, encoded_value,
                                     proof_nodes, serialized)

    @property
    def as_dict(self):
        d = self._trie.to_dict()
        return {k: rlp_decode(v)[0] for k, v in d.items()}

    @property
    def headHash(self):
        """
        The hash of the current head of the state, if the state is a merkle
        tree then hash of the root
        :return:
        """
        return self._trie.root_hash

    @property
    def committedHeadHash(self):
        return self._kv.get(self.rootHashKey)

    @property
    def isEmpty(self):
        return self.committedHeadHash == BLANK_ROOT

    def close(self):
        if self._kv:
            self._kv.close()
            self._kv = None
