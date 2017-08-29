import os
import random
import string
import types

from common.serializers.msgpack_serializer import MsgPackSerializer
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from ledger.hash_stores.file_hash_store import FileHashStore
from ledger.ledger import Ledger
from ledger.util import STH
from storage.binary_serializer_based_file_store import BinarySerializerBasedFileStore
from storage.chunked_file_store import ChunkedFileStore
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
from storage.text_file_store import TextFileStore


def random_txn(i: int):
    return {
        'identifier': 'cli' + str(i),
        'reqId': i + 1,
        'op': ''.join([random.choice(string.ascii_letters) for _ in range(
            random.randint(i + 1, 100))])
    }


def checkLeafInclusion(verifier, leafData, leafIndex, proof, treeHead):
    assert verifier.verify_leaf_inclusion(
        leaf=leafData,
        leaf_index=leafIndex,
        proof=proof,
        sth=STH(**treeHead))


def checkConsistency(tree, verifier):
    vectors = [(1, 2),
               (1, 3),
               (4, 5),
               (2, 3),
               (3, 8)]

    for oldsize, newsize in vectors:
        proof = tree.consistency_proof(oldsize, newsize)
        oldroot = tree.merkle_tree_hash(0, oldsize)
        newroot = tree.merkle_tree_hash(0, newsize)

        assert verifier.verify_tree_consistency(old_tree_size=oldsize,
                                                new_tree_size=newsize,
                                                old_root=oldroot,
                                                new_root=newroot,
                                                proof=proof)


def check_ledger_generator(ledger):
    size = ledger.size
    assert isinstance(ledger.getAllTxn(frm=1, to=size), types.GeneratorType)
    assert isinstance(ledger.getAllTxn(frm=1), types.GeneratorType)
    assert isinstance(ledger.getAllTxn(to=size), types.GeneratorType)
    assert isinstance(ledger.getAllTxn(), types.GeneratorType)


class NoTransactionRecoveryLedger(Ledger):
    def recoverTreeFromTxnLog(self):
        pass


def create_ledger(request, txn_serializer, hash_serializer, tempdir, init_genesis_txn_file=None):
    if request.param == 'TextFileStorage':
        return create_ledger_text_file_storage(txn_serializer, hash_serializer, tempdir, init_genesis_txn_file)
    elif request.param == 'ChunkedFileStorage':
        return create_ledger_chunked_file_storage(txn_serializer, hash_serializer, tempdir, init_genesis_txn_file)
    elif request.param == 'LeveldbStorage':
        return create_ledger_leveldb_file_storage(txn_serializer, hash_serializer, tempdir, init_genesis_txn_file)


def create_ledger_text_file_storage(txn_serializer, hash_serializer, tempdir, init_genesis_txn_file=None):
    if isinstance(txn_serializer, MsgPackSerializer):
        # MsgPack is a binary one, not compatible with TextFileStorage
        store = BinarySerializerBasedFileStore(txn_serializer,
                                               tempdir,
                                               'transactions',
                                               isLineNoKey=True,
                                               storeContentHash=False,
                                               ensureDurability=False)
    else:
        store = TextFileStore(tempdir,
                              'transactions',
                              isLineNoKey=True,
                              storeContentHash=False,
                              ensureDurability=False)

    return __create_ledger(store, txn_serializer, hash_serializer, tempdir, init_genesis_txn_file)


def create_ledger_leveldb_file_storage(txn_serializer, hash_serializer, tempdir, init_genesis_txn_file=None):
    store = KeyValueStorageLeveldbIntKeys(tempdir,
                                          'transactions')
    return __create_ledger(store, txn_serializer, hash_serializer, tempdir, init_genesis_txn_file)


def create_ledger_chunked_file_storage(txn_serializer, hash_serializer, tempdir, init_genesis_txn_file=None):
    chunk_creator = None
    db_name = 'transactions'
    if isinstance(txn_serializer, MsgPackSerializer):
        # TODO: fix chunk_creator support
        def chunk_creator(name):
            return BinarySerializerBasedFileStore(txn_serializer,
                                                  os.path.join(tempdir, db_name),
                                                  name,
                                                  isLineNoKey=True,
                                                  storeContentHash=False,
                                                  ensureDurability=False)
    store = ChunkedFileStore(tempdir,
                             db_name,
                             isLineNoKey=True,
                             chunkSize=5,
                             chunk_creator=chunk_creator,
                             storeContentHash=False,
                             ensureDurability=False)
    return __create_ledger(store, txn_serializer, hash_serializer, tempdir, init_genesis_txn_file)


def __create_ledger(store, txn_serializer, hash_serializer, tempdir, init_genesis_txn_file=None):
    genesis_txn_initiator = GenesisTxnInitiatorFromFile(tempdir,
                                                        init_genesis_txn_file) if init_genesis_txn_file else None
    ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(dataDir=tempdir)),
                    dataDir=tempdir,
                    transactionLogStore=store,
                    txn_serializer=txn_serializer,
                    hash_serializer=hash_serializer,
                    genesis_txn_initiator=genesis_txn_initiator)
    return ledger


def create_default_ledger(tempdir, init_genesis_txn_file=None):
    genesis_txn_initiator = GenesisTxnInitiatorFromFile(tempdir,
                                                        init_genesis_txn_file) if init_genesis_txn_file else None
    ledger = Ledger(CompactMerkleTree(hashStore=FileHashStore(dataDir=tempdir)),
                    dataDir=tempdir,
                    genesis_txn_initiator=genesis_txn_initiator)
    return ledger
