from patricia.pruning_trie import Trie, BLANK_ROOT, bin_to_nibbles
from patricia.persistent_db import PeristentDB
from patricia.refcount_db import RefcountDB
from patricia.fast_rlp import encode_optimized as rlp_encode, \
    decode_optimized as rlp_decode
from patricia.utils import to_string


class State:
    def __init__(self, db, initState):
        pass

    def set(self, key: bytes, value: bytes):
        pass

    def get(self, key: bytes, isCommitted: bool=True):
        # If `isCommitted` is True then get value corresponding to the
        # committed state else get the latest value
        pass

    def remove(self, key: bytes):
        pass

    @property
    def head(self):
        # The current head of the state, if the state is a merkle tree then
        # head is the root
        pass

    @property
    def committedHead(self):
        # The committed head of the state, if the state is a merkle tree then
        # head is the root
        pass

    def revertToCommittedHead(self):
        # Make the current head same as the committed head
        pass


class PruningState(State):
    # This key is used to store the committed root hash of the trie in the db.
    #  The committed root hash is only updated once a batch gets written to the
    #  ledger. It might happen the a few batches are in 3 phase commit and the
    #  node crashes. Now when the node restarts, it restores the db from the
    # committed root hash and all entries for uncommitted batches will be
    # ignored

    # some key that does not collide with any state variable's name
    rootHashKey = b'\x88\xc8\x88 \x9a\xa7\x89\x1b'

    def __init__(self, dbPath, initState):
        db = PeristentDB(dbPath)
        if self.rootHashKey in db:
            rootHash = db.get(self.rootHashKey)
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
        return self.trie._decode_to_node(self.committedHeadHash)

    def set(self, key: bytes, value: bytes):
        self.trie.update(key, rlp_encode([value]))

    def get(self, key: bytes, isCommitted: bool=True):
        if not isCommitted:
            return self.trie.get(key)
        else:
            return self.trie._get(self.committedHead,
                                  bin_to_nibbles(to_string(key)))

    def commit(self, rootHash=None, rootNode=None):
        if rootNode:
            rootHash = self.trie._encode_node(rootNode)
        self.db.db.put(self.rootHashKey, rootHash)

    @property
    def committedHeadHash(self):
        return self.db.db.get(self.rootHashKey)
