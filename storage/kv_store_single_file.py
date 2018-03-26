import os
from hashlib import sha256

from storage.kv_store_file import KeyValueStorageFile


class SingleFileStore(KeyValueStorageFile):

    def __init__(self,
                 dbDir,
                 dbName,
                 delimiter,
                 lineSep,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 ensureDurability: bool=True,
                 open=True):
        self.delimiter = delimiter
        self.lineSep = lineSep
        super().__init__(dbDir,
                         dbName,
                         isLineNoKey,
                         storeContentHash,
                         ensureDurability,
                         open=open)

    def put(self, key, value):
        # If line no is not treated as key then write the key and then the
        # delimiter
        if not self.isLineNoKey:
            if key is None:
                raise ValueError("Key must be provided for storing the value")
            self.db_file.write(key)
            self.db_file.write(self.delimiter)

        self.db_file.write(value)

        if self.storeContentHash:
            self.db_file.write(self.delimiter)
            if isinstance(value, str):
                value = value.encode()
            hexedHash = sha256(value).hexdigest()
            self.db_file.write(hexedHash)
        self.db_file.write(self.lineSep)

        # A little bit smart strategy like flush every 2 seconds
        # or every 10 writes or every 1 KB may be a better idea
        # Make sure data get written to the disk
        # Even flush slows down writes significantly
        self.db_file.flush()

        if self.ensureDurability:
            # fsync takes too much time on Windows.
            # This is the reason of test_merkle_proof tests slowness on Windows.
            # Even on Linux using fsync slows down the test by at least 2
            # orders of magnitude. See testMeasureWriteTime
            os.fsync(self.db_file.fileno())

    def close(self):
        self.db_file.close()

    @property
    def closed(self):
        return self.db_file.closed

    def reset(self):
        self.db_file.truncate(0)

    def drop(self):
        self.reset()

    def _parse_line(self, line, prefix=None, returnKey: bool=True,
                    returnValue: bool=True, key=None):
        if self.isLineNoKey:
            k = key
            v = line
        else:
            k, v = line.split(self.delimiter, 1)
        if returnValue:
            if self.storeContentHash:
                value, _ = v.rsplit(self.delimiter, 1)
            else:
                value = v
        if not prefix or k.startswith(prefix):
            if returnKey and returnValue:
                return k, value
            elif returnKey:
                return k
            elif returnValue:
                return value
