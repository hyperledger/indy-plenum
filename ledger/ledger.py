import logging
import time

import base58
from common.serializers.mapping_serializer import MappingSerializer
from common.serializers.serialization import ledger_txn_serializer, ledger_hash_serializer
from ledger.genesis_txn.genesis_txn_initiator import GenesisTxnInitiator
from ledger.immutable_store import ImmutableStore
from ledger.merkle_tree import MerkleTree
from ledger.tree_hasher import TreeHasher
from ledger.util import F, ConsistencyVerificationFailed
from storage.kv_store import KeyValueStorage
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys


class Ledger(ImmutableStore):
    @staticmethod
    def _defaultStore(dataDir,
                      logName,
                      ensureDurability,
                      open=True) -> KeyValueStorage:
        return KeyValueStorageLeveldbIntKeys(dataDir, logName, open)

    def __init__(self,
                 tree: MerkleTree,
                 dataDir: str,
                 txn_serializer: MappingSerializer = None,
                 hash_serializer: MappingSerializer = None,
                 fileName: str = None,
                 ensureDurability: bool = True,
                 transactionLogStore: KeyValueStorage = None,
                 genesis_txn_initiator: GenesisTxnInitiator = None):
        """
        :param tree: an implementation of MerkleTree
        :param dataDir: the directory where the transaction log is stored
        :param serializer: an object that can serialize the data before hashing
        it and storing it in the MerkleTree
        :param fileName: the name of the transaction log file
        :param genesis_txn_initiator: file or dir to use for initialization of transaction log store
        """
        self.genesis_txn_initiator = genesis_txn_initiator

        self.dataDir = dataDir
        self.tree = tree
        self.txn_serializer = txn_serializer or ledger_txn_serializer  # type: MappingSerializer
        # type: MappingSerializer
        self.hash_serializer = hash_serializer or ledger_hash_serializer
        self.hasher = TreeHasher()
        self._transactionLog = None  # type: KeyValueStorage
        self._transactionLogName = fileName or "transactions"
        self.ensureDurability = ensureDurability
        self._customTransactionLogStore = transactionLogStore
        self.seqNo = 0
        self.start()
        self.recoverTree()
        if self.genesis_txn_initiator and self.size == 0:
            self.genesis_txn_initiator.init_ledger_from_genesis_txn(self)

    def recoverTree(self):
        # TODO: Should probably have 2 classes of hash store,
        # persistent and non persistent

        start = time.perf_counter()
        if not self.tree.hashStore \
                or not self.tree.hashStore.is_persistent \
                or self.tree.leafCount == 0:
            logging.debug("Recovering tree from transaction log")
            self.recoverTreeFromTxnLog()
        else:
            try:
                logging.debug("Recovering tree from hash store of size {}".
                              format(self.tree.leafCount))
                self.recoverTreeFromHashStore()
            except ConsistencyVerificationFailed:
                logging.error("Consistency verification of merkle tree "
                              "from hash store failed, "
                              "falling back to transaction log")
                self.recoverTreeFromTxnLog()

        end = time.perf_counter()
        t = end - start
        logging.debug("Recovered tree in {} seconds".format(t))

    def recoverTreeFromTxnLog(self):
        # TODO: in this and some other lines specific fields of
        self.tree.reset()
        self.seqNo = 0
        for key, entry in self._transactionLog.iterator():
            if self.txn_serializer != self.hash_serializer:
                entry = self.serialize_for_tree(
                    self.txn_serializer.deserialize(entry))
            if isinstance(entry, str):
                entry = entry.encode()
            self._addToTreeSerialized(entry)

    def recoverTreeFromHashStore(self):
        treeSize = self.tree.leafCount
        self.seqNo = treeSize
        hashes = list(reversed(self.tree.inclusion_proof(treeSize,
                                                         treeSize + 1)))
        self.tree._update(self.tree.leafCount, hashes)
        self.tree.verify_consistency(self._transactionLog.size)

    def add(self, leaf):
        """
        Add the leaf (transaction) to the log and the merkle tree.

        Note: Currently data is serialised same way for inserting it in the
        log as well as the merkle tree, only difference is the tree needs
        binary data to the textual (utf-8) representation is converted
        to bytes.
        """
        # Serializing here to avoid serialisation in `_addToStore` and
        # `_addToTree`
        serz_leaf = self.serialize_for_txn_log(leaf)
        self._addToStore(serz_leaf, serialized=True)

        serz_leaf_for_tree = self.serialize_for_tree(leaf)
        merkle_info = self._addToTree(serz_leaf_for_tree, serialized=True)

        return merkle_info

    def _addToTree(self, leafData, serialized=False):
        serializedLeafData = self.serialize_for_tree(leafData) if \
            not serialized else leafData
        return self._addToTreeSerialized(serializedLeafData)

    def _addToStore(self, data, serialized=False):
        key = str(self.seqNo + 1)
        value = self.serialize_for_txn_log(data) if not serialized else data
        self._transactionLog.put(key=key, value=value)

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

    def append(self, txn):
        return self.add(txn)

    # TODO: add tests for this
    def get(self, **kwargs):
        for seqNo, value in self._transactionLog.iterator():
            data = self.txn_serializer.deserialize(value)
            # If `kwargs` is a subset of `data`
            if set(kwargs.values()) == {data.get(k) for k in kwargs.keys()}:
                data[F.seqNo.name] = int(seqNo)
                return data

    def getBySeqNo(self, seqNo):
        key = str(seqNo)
        value = self._transactionLog.get(key)
        if value:
            data = self.txn_serializer.deserialize(value)
            data[F.seqNo.name] = int(seqNo)
            return data
        else:
            return value

    def __getitem__(self, seqNo):
        return self.getBySeqNo(seqNo)

    def serialize_for_txn_log(self, leafData):
        return self.txn_serializer.serialize(leafData, toBytes=self._transactionLog.is_byte)

    def serialize_for_tree(self, leafData):
        return self.hash_serializer.serialize(leafData, toBytes=True)

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
        auditPath = self.tree.inclusion_proof(seqNo - 1, seqNo)
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
                                   ensureDurability)
            if self._transactionLog.closed:
                self._transactionLog.open()
            if self.tree.hashStore.closed:
                self.tree.hashStore.open()

    def stop(self):
        self._transactionLog.close()
        self.tree.hashStore.close()

    def reset(self):
        # THIS IS A DESTRUCTIVE ACTION
        self._transactionLog.reset()
        self.tree.hashStore.reset()

    # TODO: rename getAllTxn to get_txn_slice with required parameters frm to
    # add get_txn_all without args.
    def getAllTxn(self, frm: int = None, to: int = None):
        yield from ((int(seq_no), self.txn_serializer.deserialize(txn))
                    for seq_no, txn in self._transactionLog.iterator(start=frm, end=to))

    @staticmethod
    def hashToStr(h):
        return base58.b58encode(h)

    @staticmethod
    def strToHash(s):
        return base58.b58decode(s)
