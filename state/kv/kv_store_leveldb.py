from typing import Iterable, Tuple

import shutil
from state.kv.kv_store import KeyValueStorage
from state.util.utils import removeLockFiles

try:
    import leveldb
except ImportError:
    print('Cannot import leveldb, please install')


class KeyValueStorageLeveldb(KeyValueStorage):
    def __init__(self, dbPath, open=True):
        if 'leveldb' not in globals():
            raise RuntimeError('Leveldb is needed to use this class')
        self._dbPath = dbPath
        self._db = None
        if open:
            self.open()

    def __repr__(self):
        return self._dbPath

    def iter(self, start=None, end=None, include_value=True):
        return self._db.RangeIter(key_from=start, key_to=end, include_value=include_value)

    def put(self, key, value):
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

    def setBatch(self, batch: Iterable[Tuple]):
        b = leveldb.WriteBatch()
        for key, value in batch:
            if isinstance(key, str):
                key = key.encode()
            if isinstance(value, str):
                value = value.encode()
            b.Put(key, value)
        self._db.Write(b, sync=False)

    def open(self):
        self._db = leveldb.LevelDB(self._dbPath)

    def close(self):
        removeLockFiles(self._dbPath)
        del self._db
        self._db = None

    def drop(self):
        self.close()
        shutil.rmtree(self._dbPath)
