from ledger.immutable_store.stores import TextFileStore
from plenum.common.exceptions import DataDirectoryNotFound, DBConfigNotFound
from plenum.common.txn import StorageType
from plenum.common.types import Reply
from plenum.persistence.orientdb_store import OrientDbStore


class Storage:

    def start(self, loop):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    async def append(self, identifier: str, reply: Reply, txnId: str):
        raise NotImplementedError()

    async def get(self, identifier: str, reqId: int):
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()


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
