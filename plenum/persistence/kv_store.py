import shutil
from abc import abstractmethod
from typing import Tuple, Iterable

try:
    import leveldb
except ImportError:
    print('Cannot import leveldb, please install')

from plenum.persistence.util import removeLockFiles


class KVStore:
    @abstractmethod
    def set(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError

    @abstractmethod
    def remove(self, key):
        raise NotImplementedError

    @abstractmethod
    def setBatch(self, batch: Iterable[Tuple]):
        raise NotImplementedError

    @abstractmethod
    def open(self):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError


class KVStoreLeveldb:
    def __init__(self, dbPath):
        if 'leveldb' not in globals():
            raise RuntimeError('Leveldb is needed to use this class')

        self._dbPath = dbPath
        self._db = None
        self.open()

    def __repr__(self):
        return self._dbPath

    def iter(self, start=None, end=None, include_value=True):
        return self._db.RangeIter(key_from=start, key_to=end, include_value=include_value)

    def set(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        if isinstance(value, str):
            value = value.encode()
        self._db.Put(key, value)

    def get(self, key):
        if isinstance(key, str):
            key = key.encode()
        return self._db.Get(key)

    def remove(self, key):
        if isinstance(key, str):
            key = key.encode()
        self._db.Delete(key)

    @property
    def size(self):
        c = 0
        for _ in self.iter(include_value=False):
            c += 1
        return c

    def setBatch(self, batch: Iterable[Tuple]):
        b = leveldb.WriteBatch()
        for key, value in batch:
            if isinstance(key, str):
                key = key.encode()
            if isinstance(value, str):
                value = value.encode()
            b.Put(key, value)
        self._db.Write(b, sync=False)

    @property
    def closed(self):
        return self._db is None

    def open(self):
        self._db = leveldb.LevelDB(self._dbPath)

    def close(self):
        removeLockFiles(self._dbPath)
        del self._db
        self._db = None

    def drop(self):
        self.close()
        shutil.rmtree(self._dbPath)


# TODO: WIP below
class KVStoreRocksdb:
    def set(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def remove(self, key):
        raise NotImplementedError

    def setBatch(self, batch: Iterable[Tuple]):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
