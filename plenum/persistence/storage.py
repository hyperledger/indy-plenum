from abc import abstractmethod, ABC

from ledger.stores.text_file_store import TextFileStore
from plenum.common.exceptions import DataDirectoryNotFound, DBConfigNotFound
from plenum.common.txn import StorageType
from plenum.common.types import Reply
from plenum.persistence.orientdb_store import OrientDbStore


class Storage(ABC):
    @abstractmethod
    def start(self, loop):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    async def append(self, identifier: str, reply: Reply, txnId: str):
        pass

    @abstractmethod
    async def get(self, identifier: str, reqId: int, **kwargs):
        pass


def initStorage(storageType, name, dataDir=None, config=None):
    if storageType == StorageType.File:
        if dataDir is None:
            raise DataDirectoryNotFound
        return TextFileStore(dataDir, name)
    elif storageType == StorageType.OrientDB:
        if config is None:
            raise DBConfigNotFound
        orientConf = config.OrientDB
        return OrientDbStore(user=orientConf["user"],
                             password=orientConf["password"],
                             host=orientConf["host"],
                             port=orientConf["port"],
                             dbName=name)
