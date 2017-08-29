from abc import abstractmethod, ABC

from plenum.common.constants import StorageType, KeyValueStorageType
from plenum.common.exceptions import DataDirectoryNotFound, KeyValueStorageConfigNotFound
from plenum.common.messages.node_messages import Reply
from storage.kv_in_memory import KeyValueStorageInMemory
from storage.kv_store import KeyValueStorage
from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.text_file_store import TextFileStore


class Storage(ABC):
    @abstractmethod
    def start(self, loop):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    async def append(self, reply: Reply):
        pass

    @abstractmethod
    async def get(self, identifier: str, reqId: int, **kwargs):
        pass


def initKeyValueStorage(keyValueType, dataLocation,
                        keyValueStorageName) -> KeyValueStorage:
    if keyValueType == KeyValueStorageType.Leveldb:
        return KeyValueStorageLeveldb(dataLocation, keyValueStorageName)
    elif keyValueType == KeyValueStorageType.Memory:
        return KeyValueStorageInMemory()
    else:
        raise KeyValueStorageConfigNotFound


def initStorage(storageType, name, dataDir=None, config=None):
    if storageType == StorageType.File:
        if dataDir is None:
            raise DataDirectoryNotFound
        return TextFileStore(dataDir, name)
