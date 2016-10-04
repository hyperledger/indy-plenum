from pathlib import Path

class RequestIdStore:

    from abc import abstractmethod

    @abstractmethod
    def nextId(self, clientId, signerId) -> int:
        pass

    @abstractmethod
    def currentId(self, clientId, signerId) -> int:
        pass

class FileRequestIdStore(RequestIdStore):

    def __init__(self, filePath):
        self.isOpen = False
        self.storeFilePath = filePath
        self._storage = {}

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
        self._storage = {}
        self.isOpen = False

    def flush(self):
        if self.isOpen:
            self._saveStorage()

    def _loadStorage(self):
        my_file = Path("/path/to/file")
        if my_file.exists():
            with open(self.storeFilePath) as file:
                for line in file:
                    (clientId, signerId, lastRequest) = line.split(";")
                    self._storage[clientId, signerId] = int(lastRequest)

    def _saveStorage(self):
        with open(self.storeFilePath, 'w') as file:
            for (clientId, signerId), lastRequest in self._storage.items():
                file.write("{};{};{}\n".format(clientId, signerId, lastRequest))

    def nextId(self, clientId, signerId) -> int:
        lastRequestId = self._storage.get((clientId, signerId))
        nextId = lastRequestId + 1 if lastRequestId is not None else 0
        self._storage[clientId, signerId] = nextId
        return nextId

    def currentId(self, clientId, signerId) -> int:
        return self._storage.get((clientId, signerId))
