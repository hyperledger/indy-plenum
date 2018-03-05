import os

from typing import Iterable, Tuple

import shutil
from storage.kv_store import KeyValueStorage
from state.util.utils import removeLockFiles

try:
    import rocksdb
except ImportError:
    print('Cannot import rocksdb, please install')


class KeyValueStorageRocksdb(KeyValueStorage):
    def __init__(self, db_dir, db_name, open=True):
        if 'rocksdb' not in globals():
            raise RuntimeError('Rocksdb is needed to use this class')
        self._db_path = os.path.join(db_dir, db_name)
        self._db = None
        if open:
            self.open()

    def open(self):
        self._db = rocksdb.DB(self._db_path, rocksdb.Options(create_if_missing=True))

    def __repr__(self):
        return self._db_path

    def db_path(self) -> str:
        return self._db_path

    def put(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        if isinstance(value, str):
            value = value.encode()
        self._db.put(key, value)

    def get(self, key):
        if isinstance(key, str):
            key = key.encode()
        vv = self._db.get(key)
        if vv is None:
            raise KeyError
        return vv

    def remove(self, key):
        if isinstance(key, str):
            key = key.encode()
        self._db.delete(key)

    def setBatch(self, batch: Iterable[Tuple]):
        b = rocksdb.WriteBatch()
        for key, value in batch:
            if isinstance(key, str):
                key = key.encode()
            if isinstance(value, str):
                value = value.encode()
            b.put(key, value)
        self._db.write(b, sync=False)

    def close(self):
        del self._db
        self._db = None
        removeLockFiles(self._db_path)

    def drop(self):
        self.close()
        shutil.rmtree(self._db_path)

    def reset(self):
        self.drop()
        self.open()

    def iter(self, start=None, end=None, include_value=True):
        if not include_value:
            itr = self._db.iterkeys()
        else:
            itr = self._db.iteritems()
        itr.seek_to_first()
        return itr

    def has_key(self, key):
        if isinstance(key, str):
            key = key.encode()
        return self._db.key_may_exist(key)[0]

    @property
    def closed(self):
        return self._db is None
