import logging
import os
import shutil
from abc import abstractmethod
from hashlib import sha256

from typing import Tuple, Iterable

from storage.kv_store import KeyValueStorage


class KeyValueStorageFile(KeyValueStorage):
    """
    A file based implementation of a key value store.
    """

    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 ensureDurability: bool=True,
                 open=True):
        """
        :param dbDir: The directory where the file storing the data would be
        present
        :param dbName: The name of the file that is used to store the data
        :param isLineNoKey: If false then each line has the key followed by a
        delimiter followed by the value
        :param storeContentHash: Whether to store a hash of the value or not.
        Storing hash can make it really fast to compare the value for equality
        :param ensureDurability: Should the file be fysnced after every write.
        This can ensure durability in most of the cases, but make
        writes extremely slow. See testMeasureWriteTime. For frequent writes,
        it makes sense to disable flush and fsync on every write
        """
        self.isLineNoKey = isLineNoKey
        self.storeContentHash = storeContentHash
        self.ensureDurability = ensureDurability
        self._db_dir = dbDir
        self._db_name = dbName
        if open:
            self.open()

    def _initDB(self, dbDir, dbName):
        if not os.path.exists(dbDir):
            os.makedirs(dbDir)
        self.db_path = self._init_db_path(dbDir, dbName)
        self.db_file = self._init_db_file()

    def db_path(self) -> str:
        return self.db_path

    @abstractmethod
    def _init_db_path(self, dbDir, dbName):
        pass

    @abstractmethod
    def _init_db_file(self):
        pass

    def open(self):
        self._initDB(self._db_dir, self._db_name)
        self._append_new_line_if_req()

    def get(self, key):
        for k, v in self.iterator():
            if k == key:
                return v
        raise KeyError("'{}' doesn't contain {} key".format(
            self.db_file, str(key)))

    def iterator(self, start=None, end=None, include_key=True, include_value=True, prefix=None):
        if not (include_key or include_value):
            raise ValueError("At least one of includeKey or includeValue "
                             "should be true")
        lines = self._lines()
        if include_key and include_value:
            return self._keyValueIterator(lines, start=start, end=end, prefix=prefix)
        elif include_value:
            return self._valueIterator(lines, start=start, end=end, prefix=prefix)
        else:
            return self._keyIterator(lines, start=start, end=end, prefix=prefix)

    def remove(self, key):
        raise NotImplementedError

    def setBatch(self, batch: Iterable[Tuple]):
        for k, v in batch:
            self.put(k, v)

    def do_ops_in_batch(self, batch: Iterable[Tuple]):
        for op, key, value in batch:
            if op == self.WRITE_OP:
                self.put(key, value)
            elif op == self.REMOVE_OP:
                self.remove(key)
            else:
                raise ValueError('Unknown operation')

    def _is_valid_range(self, start=None, end=None):
        if start and end:
            assert self.isLineNoKey
            if start > end:
                raise ValueError("range [{}, {}] is invalid".format(start, end))

    def _keyIterator(self, lines, start=None, end=None, prefix=None):
        return self._baseIterator(lines, start, end, prefix, True, False)

    def _valueIterator(self, lines, start=None, end=None, prefix=None):
        return self._baseIterator(lines, start, end, prefix, False, True)

    def _keyValueIterator(self, lines, start=None, end=None, prefix=None):
        return self._baseIterator(lines, start, end, prefix, True, True)

    def _baseIterator(self, lines, start=None, end=None, prefix=None, returnKey: bool=True, returnValue: bool=True):
        self._is_valid_range(start, end)
        i = 1
        for line in lines:
            k = str(i)
            if (start is None or i >= start) and (end is None or i <= end):
                yield self._parse_line(line, prefix, returnKey, returnValue, k)
            if end is not None and i > end:
                break
            if self.isLineNoKey:
                i += 1

    @abstractmethod
    def _lines(self):
        pass

    @abstractmethod
    def _parse_line(self, line, prefix=None, returnKey: bool=True,
                    returnValue: bool=True, key=None):
        pass

    @abstractmethod
    def _append_new_line_if_req(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
