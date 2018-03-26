import logging
import os

from storage.kv_store_single_file import SingleFileStore


class BinaryFileStore(SingleFileStore):
    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 ensureDurability: bool=True,
                 open=True):
        # This is the separator between key and value
        # TODO: This line separator might conflict with some data format.
        # So prefix the value data in the file with size and only read those
        # number of bytes.
        super().__init__(dbDir,
                         dbName,
                         delimiter=b"\t",
                         lineSep=b'\n\x07\n\x01',
                         isLineNoKey=isLineNoKey,
                         storeContentHash=storeContentHash,
                         ensureDurability=ensureDurability,
                         open=open)

    @property
    def is_byte(self) -> bool:
        return True

    @staticmethod
    def _isBytes(arg):
        return isinstance(arg, (bytes, bytearray))

    def _init_db_path(self, dbDir, dbName):
        return os.path.join(dbDir, "{}.bin".format(dbName))

    def _init_db_file(self):
        return open(self.db_path, mode="a+b", buffering=0)

    def put(self, key, value):
        if not ((self.isLineNoKey or not key or self._isBytes(key)) and self._isBytes(value)):
            raise ValueError("key and value need to be bytes-like object")
        super().put(key=key, value=value)

    def get(self, key):
        if not self.isLineNoKey and not self._isBytes(key):
            raise TypeError("key needs to be a bytes-like object")
        return super().get(key)

    def iterator(self, start=None, end=None, includeKey=True, includeValue=True, prefix=None):
        if prefix and not self._isBytes(prefix):
            raise TypeError("prefix needs to be a bytes-like object")

        return super().iterator(start, end, includeKey, includeValue, prefix)

    def _lines(self):
        # Move to the beginning of file
        self.db_file.seek(0)
        return (line.strip(self.lineSep) for line in
                self.db_file.read().split(self.lineSep)
                if len(line.strip(self.lineSep)) != 0)

    def _append_new_line_if_req(self):
        pass
