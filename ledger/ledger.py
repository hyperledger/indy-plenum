import base64
import logging
import time
from collections import OrderedDict

from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.stores.chunked_file_store import ChunkedFileStore
from ledger.tree_hasher import TreeHasher
from ledger.merkle_tree import MerkleTree
from ledger.serializers.mapping_serializer import MappingSerializer
from ledger.serializers.json_serializer import JsonSerializer
from ledger.stores.file_store import FileStore
from ledger.stores.text_file_store import TextFileStore
from ledger.immutable_store import ImmutableStore
from ledger.util import F


class Ledger(ImmutableStore):

    @staticmethod
    def _defaultStore(dataDir,
                      logName,
                      ensureDurability,
                      defaultFile) -> FileStore:

        return TextFileStore(dataDir,
                             logName,
                             isLineNoKey=True,
                             storeContentHash=False,
                             ensureDurability=ensureDurability,
                             defaultFile=defaultFile)

    def __init__(self,
                 tree: MerkleTree,
                 dataDir: str,
                 serializer: MappingSerializer=None,
                 fileName: str=None,
                 ensureDurability: bool=True,
                 transactionLogStore: FileStore=None,
                 defaultFile=None):
        """
        :param tree: an implementation of MerkleTree
        :param dataDir: the directory where the transaction log is stored
        :param serializer: an object that can serialize the data before hashing
        it and storing it in the MerkleTree
        :param fileName: the name of the transaction log file
        :param defaultFile: file or dir to use for initialization of transaction log store
        """
        assert not transactionLogStore or not defaultFile
        self.defaultFile = defaultFile

        self.dataDir = dataDir
        self.tree = tree
        self.leafSerializer = serializer or \
            JsonSerializer()  # type: MappingSerializer
        self.hasher = TreeHasher()
        self._transactionLog = None  # type: FileStore
        self._transactionLogName = fileName or "transactions"
        self.ensureDurability = ensureDurability
        self._customTransactionLogStore = transactionLogStore
        self.start()
        self.seqNo = 0
        self.recoverTree()

    def recoverTree(self):
        # TODO: Should probably have 2 classes of hash store,
        # persistent and non persistent

        # TODO: this definitely should be done in a more generic way:
        if not isinstance(self.tree, CompactMerkleTree):
            logging.error("Do not know how to recover {}".format(self.tree))
            raise TypeError("Merkle tree type {} is not supported"
                            .format(type(self.tree)))


        # ATTENTION!
        # This functionality is disabled until better consistency verification
        # implemented - always using recovery from transaction log
        # from ledger.stores.memory_hash_store import MemoryHashStore
        # from ledger.util import ConsistencyVerificationFailed
        # if not self.tree.hashStore \
        #         or isinstance(self.tree.hashStore, MemoryHashStore) \
        #         or self.tree.leafCount == 0:
        #     logging.info("Recovering tree from transaction log")
        #     self.recoverTreeFromTxnLog()
        # else:
        #     try:
        #         logging.info("Recovering tree from hash store of size {}".
        #                       format(self.tree.leafCount))
        #         self.recoverTreeFromHashStore()
        #     except ConsistencyVerificationFailed:
        #         logging.error("Consistency verification of merkle tree "
        #                       "from hash store failed, "
        #                       "falling back to transaction log")
        #         self.recoverTreeFromTxnLog()

        logging.debug("Recovering tree from transaction log")
        start = time.perf_counter()
        self.recoverTreeFromTxnLog()
        end = time.perf_counter()
        t = end - start
        logging.debug("Recovered tree from transaction log in {} seconds".
                      format(t))

    def recoverTreeFromTxnLog(self):
        # TODO: in this and some other lines specific fields of
        # CompactMerkleTree are used, but type of self.tree is MerkleTree
        # This must be fixed!
        self.tree.hashStore.reset()
        for key, entry in self._transactionLog.iterator():
            if isinstance(entry, str):
                entry = entry.encode()
            self._addToTreeSerialized(entry)

    def recoverTreeFromHashStore(self):
        treeSize = self.tree.leafCount
        self.seqNo = treeSize
        hashes = list(reversed(self.tree.inclusion_proof(treeSize,
                                                         treeSize + 1)))
        self.tree._update(self.tree.leafCount, hashes)
        self.tree.verifyConsistency(self._transactionLog.numKeys)

    def add(self, leaf):
        self._addToStore(leaf)
        merkleInfo = self._addToTree(leaf)
        return merkleInfo

    def _addToTree(self, leafData):
        serializedLeafData = self.serializeLeaf(leafData)
        return self._addToTreeSerialized(serializedLeafData)

    def _addToTreeSerialized(self, serializedLeafData):
        audit_path = self.tree.append(serializedLeafData)
        self.seqNo += 1
        return self._build_merkle_proof(audit_path)

    def _build_merkle_proof(self, audit_path):
        return {
            F.seqNo.name: self.seqNo,
            F.rootHash.name: self.hashToStr(self.tree.root_hash),
            F.auditPath.name: [self.hashToStr(h) for h in audit_path]
        }

    def _addToStore(self, data):
        key = str(self.seqNo + 1)
        self._transactionLog.put(key=key,
                                 value=self.leafSerializer.serialize(
                                     data, toBytes=False))

    def append(self, txn):
        return self.add(txn)

    def get(self, **kwargs):
        for seqNo, value in self._transactionLog.iterator():
            data = self.leafSerializer.deserialize(value)
            # If `kwargs` is a subset of `data`
            if set(kwargs.values()) == {data.get(k) for k in kwargs.keys()}:
                data[F.seqNo.name] = int(seqNo)
                return data

    def getBySeqNo(self, seqNo):
        key = str(seqNo)
        value = self._transactionLog.get(key)
        if value:
            data = self.leafSerializer.deserialize(value)
            data[F.seqNo.name] = int(seqNo)
            return data
        else:
            return value

    def __getitem__(self, seqNo):
        return self.getBySeqNo(seqNo)

    def lastCount(self):
        key = self._transactionLog.lastKey
        return 0 if key is None else int(key)

    def serializeLeaf(self, leafData):
        return self.leafSerializer.serialize(leafData)

    @property
    def size(self) -> int:
        return self.tree.tree_size

    def __len__(self):
        return self.size

    @property
    def root_hash(self) -> str:
        return self.hashToStr(self.tree.root_hash)

    def merkleInfo(self, seqNo):
        seqNo = int(seqNo)
        assert seqNo > 0
        rootHash = self.tree.merkle_tree_hash(0, seqNo)
        auditPath = self.tree.inclusion_proof(seqNo-1, seqNo)
        return {
            F.rootHash.name: self.hashToStr(rootHash),
            F.auditPath.name: [self.hashToStr(h) for h in auditPath]
        }

    def start(self, loop=None, ensureDurability=True):
        if self._transactionLog and not self._transactionLog.closed:
            logging.debug("Ledger already started.")
        else:
            logging.debug("Starting ledger...")
            ensureDurability = ensureDurability or self.ensureDurability
            self._transactionLog = \
                self._customTransactionLogStore or \
                self._defaultStore(self.dataDir,
                                   self._transactionLogName,
                                   ensureDurability,
                                   self.defaultFile)
            self._transactionLog.appendNewLineIfReq()

    def stop(self):
        self._transactionLog.close()

    def reset(self):
        # THIS IS A DESTRUCTIVE ACTION
        self._transactionLog.reset()

    def getAllTxn(self, frm: int=None, to: int=None):
        yield from ((seq_no, self.leafSerializer.deserialize(txn))
                    for seq_no, txn in self._transactionLog.get_range(frm, to))

    @staticmethod
    def hashToStr(h):
        return base64.b64encode(h).decode()

    @staticmethod
    def strToHash(s):
        return base64.b64decode(s).encode()
