import os

from ledger.stores.file_store import FileStore


class BinaryFileStore(FileStore):
    def __init__(self, dbDir, dbName, isLineNoKey: bool=False,
                 storeContentHash: bool=True, ensureDurability: bool=True):
        # This is the separator between key and value
        self.delimiter = b"\t"
        # TODO: This line separator might conflict with some data format.
        # So prefix the value data in the file with size and only read those
        # number of bytes.
        self.lineSep = b'\n\x07\n\x01'
        super().__init__(dbDir, dbName, isLineNoKey, storeContentHash,
                         ensureDurability)
        self._initDB(dbDir, dbName)

    @staticmethod
    def _isBytes(arg):
        return isinstance(arg, (bytes, bytearray))

    def _initDB(self, dbDir, dbName):
        super()._initDB(dbDir, dbName)
        self.dbPath = os.path.join(dbDir, "{}.bin".format(dbName))
        self.dbFile = open(self.dbPath, mode="a+b", buffering=0)

    def put(self, value, key=None):
        if not ((not key or self._isBytes(key)) and self._isBytes(value)):
            raise ValueError("key and value need to be bytes-like object")
        super().put(key=key, value=value)

    def get(self, key):
        if not self._isBytes(key):
            raise TypeError("key needs to be a bytes-like object")
        return super().get(key)

    def iterator(self, includeKey=True, includeValue=True, prefix=None):
        if prefix and not self._isBytes(prefix):
            raise TypeError("prefix needs to be a bytes-like object")

        return super().iterator(includeKey, includeValue, prefix)

    def _lines(self):
        return (line.strip(self.lineSep) for line in
                self.dbFile.read().split(self.lineSep)
                if len(line.strip(self.lineSep)) != 0)
