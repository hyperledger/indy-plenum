import os

from ledger.hash_stores.file_hash_store import FileHashStore
from ledger.hash_stores.hash_store import HashStore
from ledger.hash_stores.memory_hash_store import MemoryHashStore

from plenum.common.config_util import getConfig
from plenum.common.constants import KeyValueStorageType, HS_FILE, HS_LEVELDB, HS_ROCKSDB
from plenum.common.exceptions import KeyValueStorageConfigNotFound

from plenum.persistence.db_hash_store import DbHashStore
from storage.binary_file_store import BinaryFileStore
from storage.binary_serializer_based_file_store import BinarySerializerBasedFileStore
from storage.chunked_file_store import ChunkedFileStore

from storage.kv_in_memory import KeyValueStorageInMemory
from storage.kv_store import KeyValueStorage


def initKeyValueStorage(keyValueType, dataLocation, keyValueStorageName,
                        open=True, read_only=False, db_config=None, txn_serializer=None) -> KeyValueStorage:
    from storage.kv_store_leveldb import KeyValueStorageLeveldb
    from storage.kv_store_rocksdb import KeyValueStorageRocksdb

    if keyValueType == KeyValueStorageType.Leveldb:
        return KeyValueStorageLeveldb(dataLocation, keyValueStorageName, open,
                                      read_only)

    if keyValueType == KeyValueStorageType.Rocksdb:
        return KeyValueStorageRocksdb(dataLocation, keyValueStorageName, open,
                                      read_only, db_config)

    if keyValueType == KeyValueStorageType.Memory:
        return KeyValueStorageInMemory()

    if keyValueType == KeyValueStorageType.ChunkedBinaryFile:
        def chunk_creator(name):
            return BinarySerializerBasedFileStore(txn_serializer,
                                                  os.path.join(dataLocation, keyValueStorageName),
                                                  name,
                                                  isLineNoKey=True,
                                                  storeContentHash=False,
                                                  ensureDurability=False)
        return ChunkedFileStore(dataLocation,
                                keyValueStorageName,
                                isLineNoKey=True,
                                chunkSize=5,
                                chunk_creator=chunk_creator,
                                storeContentHash=False,
                                ensureDurability=False)

    if keyValueType == KeyValueStorageType.BinaryFile:
        return BinaryFileStore(dataLocation, keyValueStorageName,
                               delimiter=b'\0x8b\0xad\0xf0\0x0d\0x8b\0xad\0xf0\0x0d',
                               lineSep=b'\0xde\0xad\0xbe\0xef\0xde\0xad\0xbe\0xef',
                               storeContentHash=False)

    raise KeyValueStorageConfigNotFound


def initKeyValueStorageIntKeys(keyValueType, dataLocation, keyValueStorageName,
                               open=True, read_only=False, db_config=None, txn_serializer=None) -> KeyValueStorage:
    from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
    from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys
    if keyValueType == KeyValueStorageType.Leveldb:
        return KeyValueStorageLeveldbIntKeys(dataLocation, keyValueStorageName, open, read_only)
    if keyValueType == KeyValueStorageType.Rocksdb:
        return KeyValueStorageRocksdbIntKeys(dataLocation, keyValueStorageName, open, read_only, db_config)
    return initKeyValueStorage(keyValueType, dataLocation, keyValueStorageName, open, read_only, db_config, txn_serializer)


def initHashStore(data_dir, name, config=None, read_only=False, hs_type=None) -> HashStore:
    """
    Create and return a hashStore implementation based on configuration
    """
    config = config or getConfig()
    hsConfig = hs_type if hs_type is not None else config.hashStore['type'].lower()
    if hsConfig == HS_FILE:
        return FileHashStore(dataDir=data_dir,
                             fileNamePrefix=name)
    elif hsConfig == HS_LEVELDB or hsConfig == HS_ROCKSDB:
        return DbHashStore(dataDir=data_dir,
                           fileNamePrefix=name,
                           db_type=hsConfig,
                           read_only=read_only,
                           config=config)
    else:
        return MemoryHashStore()


def integer_comparator(a, b):
    if a == b:
        return 0
    a = int(a)
    b = int(b)
    return 1 if a > b else -1
