from binascii import unhexlify

from plenum.persistence.state import State
from state.db.persistent_db import PersistentDB
from state.db.refcount_db import RefcountDB
from state.trie.pruning_trie import BLANK_ROOT, Trie, BLANK_NODE, bin_to_nibbles
from state.util.fast_rlp import encode_optimized as rlp_encode, \
    decode_optimized as rlp_decode
from state.util.utils import to_string
from stp_core.crypto.util import isHex


# TODO: Some PruningState's method often used in context where State expected,
# this should be fixed by moving declaration of these methods to State

class PruningState(State):
    # This class is used to store the
    # committed root hash of the trie in the db.
    # The committed root hash is only updated once a batch gets written to the
    # ledger. It might happen that a few batches are in 3 phase commit and the
    # node crashes. Now when the node restarts, it restores the db from the
    # committed root hash and all entries for uncommitted batches will be
    # ignored

    # some key that does not collide with any state variable's name
    rootHashKey = b'\x88\xc8\x88 \x9a\xa7\x89\x1b'

    def __init__(self, dbPath, initState=None):
        db = PersistentDB(dbPath)
        if self.rootHashKey in db:
            rootHash = bytes(db.get(self.rootHashKey))
        else:
            rootHash = BLANK_ROOT
            db.put(self.rootHashKey, BLANK_ROOT)
        self.db = RefcountDB(db)
        self.trie = Trie(db, rootHash)

    @property
    def head(self):
        # The current head of the state, if the state is a merkle tree then
        # head is the root
        return self.trie.root_node

    @property
    def committedHead(self):
        # The committed head of the state, if the state is a merkle tree then
        # head is the root
        if self.committedHeadHash == BLANK_ROOT:
            return BLANK_NODE
        else:
            return self.trie._decode_to_node(self.committedHeadHash)

    def set(self, key: bytes, value: bytes):
        self.trie.update(key, rlp_encode([value]))

    def get(self, key: bytes, isCommitted: bool=True):
        if not isCommitted:
            val = self.trie.get(key)
        else:
            val = self.trie._get(self.committedHead,
                                 bin_to_nibbles(to_string(key)))
        if val:
            return rlp_decode(val)[0]

    def commit(self, rootHash=None, rootNode=None):
        if rootNode:
            rootHash = self.trie._encode_node(rootNode)
        elif isHex(rootHash):
            if isinstance(rootHash, str):
                rootHash = rootHash.encode()
            rootHash = unhexlify(rootHash)
        self.db.db.put(self.rootHashKey, rootHash)

    def revertToHead(self, headHash=None):
        if headHash != BLANK_ROOT:
            head = self.trie._decode_to_node(headHash)
        else:
            head = BLANK_NODE
        self.trie.replace_root_hash(self.trie.root_node, head)

    @property
    def as_dict(self):
        d = self.trie.to_dict()
        return {k: rlp_decode(v)[0] for k, v in d.items()}

    @property
    def headHash(self):
        """
        The hash of the current head of the state, if the state is a merkle
        tree then hash of the root
        :return:
        """
        return self.trie.root_hash

    @property
    def committedHeadHash(self):
        return self.db.db.get(self.rootHashKey)

    @property
    def isEmpty(self):
        return self.committedHeadHash == BLANK_ROOT

    def close(self):
        self.db.db.close()
        self.db.kv = None
