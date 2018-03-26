from abc import abstractmethod, ABC

from plenum.common.constants import StorageType
from plenum.common.exceptions import DataDirectoryNotFound
from plenum.common.messages.node_messages import Reply
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


def initStorage(storageType, name, dataDir=None, config=None):
    if storageType == StorageType.File:
        if dataDir is None:
            raise DataDirectoryNotFound
        return TextFileStore(dataDir, name)
