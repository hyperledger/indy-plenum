import logging
import os
from itertools import chain

from storage.kv_store_single_file import SingleFileStore


class BinaryFileStore(SingleFileStore):
    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 ensureDurability: bool=True,
                 open=True,
                 delimiter=b'\t',
                 lineSep=b'\n\x07\n\x01'):
        # This is the separator between key and value
        # TODO: This line separator might conflict with some data format.
        # So prefix the value data in the file with size and only read those
        # number of bytes.
        super().__init__(dbDir,
                         dbName,
                         delimiter=delimiter,
                         lineSep=lineSep,
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
        key = self.to_byte_repr(key)
        value = self.to_byte_repr(value)
        if not ((self.isLineNoKey or not key or self._isBytes(key)) and self._isBytes(value)):
            raise ValueError("key and value need to be bytes-like object")
        super().put(key=key, value=value)

    def get(self, key):
        key = self.to_byte_repr(key)
        if not self.isLineNoKey and not self._isBytes(key):
            raise TypeError("key needs to be a bytes-like object")
        return super().get(key)

    def iterator(self, start=None, end=None, includeKey=True, includeValue=True, prefix=None):
        if prefix and not self._isBytes(prefix):
            raise TypeError("prefix needs to be a bytes-like object")

        return super().iterator(start, end, includeKey, includeValue, prefix)

    def _file_chunks(self):
        buf_size = 4096
        self.db_file.seek(0)
        while True:
            chunk = self.db_file.read(buf_size)
            yield chunk
            if len(chunk) < buf_size:
                break

    def _lines(self):
        buffer = bytearray()
        lines = []
        for chunk in self._file_chunks():
            buffer.extend(chunk)
            lines = buffer.split(self.lineSep)
            for line in lines[:-1]:
                yield line
            buffer = buffer[-len(lines[-1]):]
        if len(lines[-1]) > 0:
            yield lines[-1]

    def _append_new_line_if_req(self):
        pass
