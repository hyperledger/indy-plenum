from pathlib import Path
from abc import abstractmethod


class RequestIdStore:

    @abstractmethod
    def nextId(self, signerId) -> int:
        pass

    @abstractmethod
    def currentId(self, signerId) -> int:
        pass


class FileRequestIdStore(RequestIdStore):
    def __init__(self, filePath, valueSeparator = '|'):
        # isOpen shows the state of the store
        # if True then data is loaded from file (if store file exists)
        # and store can be used for getting ids.
        # Otherwise - store is closed and contains no data
        self.isOpen = False
        self.storeFilePath = filePath
        self.clear()
        self._valueSeparator = valueSeparator

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if self.isOpen:
            raise RuntimeError("Storage is already open!")
        self._loadStorage()
        self.isOpen = True

    def close(self):
        self.flush()
        self.clear()
        self.isOpen = False

    def flush(self):
        if self.isOpen:
            self._saveStorage()

    def _loadStorage(self):
        storageFile = Path(self.storeFilePath)
        if storageFile.exists():
            with storageFile.open() as file:
                for line in file:
                    (signerId, lastReqId) = line.split(self._valueSeparator)
                    self._storage[signerId] = int(lastReqId)

    def _saveStorage(self):
        with open(self.storeFilePath, 'w') as file:
            for signerId, lastReqId in self._storage.items():
                values = [str(x) for x in [signerId, lastReqId]]
                line = self._valueSeparator.join(values)
                file.write(line + "\n")

    def nextId(self, signerId) -> int:
        lastRequestId = self._storage.get(signerId) or 0
        nextId = lastRequestId + 1
        self._storage[signerId] = nextId
        self.flush()
        return nextId

    def currentId(self, signerId) -> int:
        return self._storage.get(signerId)

    def clear(self):
        self._storage = {}


class MemoryRequestIdStore(RequestIdStore):

    def __init__(self):
        self.ids = {}

    def nextId(self, signerId) -> int:
        id = (self.ids.get(signerId) or 0)
        next = id + 1
        self.ids[signerId] = next
        return next

    def currentId(self, signerId) -> int:
        return self.ids.get(signerId)