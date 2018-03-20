from ledger.hash_stores.file_hash_store import FileHashStore
from ledger.hash_stores.hash_store import HashStore
from ledger.hash_stores.memory_hash_store import MemoryHashStore

from plenum.common.config_util import getConfig
from plenum.common.constants import KeyValueStorageType, HS_FILE, HS_LEVELDB, HS_ROCKSDB
from plenum.common.exceptions import KeyValueStorageConfigNotFound

from plenum.persistence.db_hash_store import DbHashStore

from storage.kv_in_memory import KeyValueStorageInMemory
from storage.kv_store import KeyValueStorage
from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.kv_store_rocksdb import KeyValueStorageRocksdb
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys


def initKeyValueStorage(keyValueType, dataLocation,
                        keyValueStorageName) -> KeyValueStorage:
    if keyValueType == KeyValueStorageType.Leveldb:
        return KeyValueStorageLeveldb(dataLocation, keyValueStorageName)
    if keyValueType == KeyValueStorageType.Rocksdb:
        return KeyValueStorageRocksdb(dataLocation, keyValueStorageName)
    elif keyValueType == KeyValueStorageType.Memory:
        return KeyValueStorageInMemory()
    else:
        raise KeyValueStorageConfigNotFound


def initKeyValueStorageIntKeys(keyValueType, dataLocation,
                               keyValueStorageName) -> KeyValueStorage:
    if keyValueType == KeyValueStorageType.Leveldb:
        return KeyValueStorageLeveldbIntKeys(dataLocation, keyValueStorageName)
    if keyValueType == KeyValueStorageType.Rocksdb:
        return KeyValueStorageRocksdbIntKeys(dataLocation, keyValueStorageName)
    else:
        raise KeyValueStorageConfigNotFound


def initHashStore(data_dir, name, config=None) -> HashStore:
    """
    Create and return a hashStore implementation based on configuration
    """
    config = config or getConfig()
    hsConfig = config.hashStore['type'].lower()
    if hsConfig == HS_FILE:
        return FileHashStore(dataDir=data_dir,
                             fileNamePrefix=name)
    elif hsConfig == HS_LEVELDB or hsConfig == HS_ROCKSDB:
        return DbHashStore(dataDir=data_dir,
                           fileNamePrefix=name,
                           db_type=hsConfig)
    else:
        return MemoryHashStore()
